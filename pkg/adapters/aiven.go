package adapters

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtuneai/agent/pkg/adeptersinterfaces"
	"github.com/dbtuneai/agent/pkg/agent"
	"github.com/dbtuneai/agent/pkg/collectors"
	"github.com/dbtuneai/agent/pkg/internal/parameters"
	"github.com/dbtuneai/agent/pkg/internal/utils"
	"github.com/aiven/go-client-codegen"
        "github.com/aiven/go-client-codegen/handler/service"
	"github.com/hashicorp/go-retryablehttp"
	pgPool "github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

// AivenPostgreSQLAdapter represents an adapter for connecting to Aiven PostgreSQL services
type AivenPostgreSQLAdapter struct {
	DefaultPostgreSQLAdapter
	aivenConfig    adeptersinterfaces.AivenConfig
	serviceHandler service.Handler
	state          *adeptersinterfaces.AivenState
}

// aivenModifiableParams defines which PostgreSQL parameters can be modified on Aiven
// and how they should be applied (pg config or service-level config)
var aivenModifiableParams = map[string]struct {
	Modifiable  bool
	ServiceLevel bool // If true, apply at service level instead of under pg config
}{
	// Parameters confirmed modifiable through pg config
	"bgwriter_lru_maxpages":            {true, false},
	"bgwriter_delay":                   {true, false},
	"max_parallel_workers_per_gather":  {true, false},
	"max_parallel_workers":             {true, false},
	"work_mem":                         {true, true},
	"shared_buffers_percentage":        {true, true}, // Instead of shared_buffers
	"max_worker_processes":             {true, true}, // Only can be increased
	
	// Known to be restricted (for documentation)
	"max_wal_size":                    {false, false},
	"min_wal_size":                    {false, false},
	"random_page_cost":                {false, false},
	"seq_page_cost":                   {false, false},
	"checkpoint_completion_target":    {false, false},
	"effective_io_concurrency":        {false, false},
	"shared_buffers":                  {false, false}, // Use shared_buffers_percentage instead
}

// Helper function to create bool pointers for API requests
func boolPtr(b bool) *bool {
	return &b
}

// CreateAivenPostgreSQLAdapter creates a new Aiven PostgreSQL adapter
func CreateAivenPostgreSQLAdapter() (*AivenPostgreSQLAdapter, error) {
	// Create a new Viper instance for Aiven configuration if the sub-config doesn't exist
	dbtuneConfig := viper.Sub("aiven")
	if dbtuneConfig == nil {
		// If the section doesn't exist in the config file, create a new Viper instance
		dbtuneConfig = viper.New()
	}

	// Bind environment variables
	dbtuneConfig.BindEnv("api_token", "DBT_AIVEN_API_TOKEN")
	dbtuneConfig.BindEnv("project_name", "DBT_AIVEN_PROJECT_NAME")
	dbtuneConfig.BindEnv("service_name", "DBT_AIVEN_SERVICE_NAME")

	var aivenConfig adeptersinterfaces.AivenConfig
	err := dbtuneConfig.Unmarshal(&aivenConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to decode into struct: %v", err)
	}

	// Validate required configuration
	err = utils.ValidateStruct(&aivenConfig)
	if err != nil {
		return nil, err
	}

	// Create Aiven client
	client, err := aiven.NewClient([]aiven.Option{
		aiven.TokenOpt(aivenConfig.APIToken),
	}...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Aiven client: %v", err)
	}
	
	// Create service handler
	serviceHandler := service.NewHandler(client)
	
	// Verify connectivity by trying to get the service
	_, err = serviceHandler.ServiceGet(
		context.Background(),
		aivenConfig.ProjectName,
		aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Aiven API: %v", err)
	}

	// Create default PostgreSQL adapter as base
	defaultAdapter, err := CreateDefaultPostgreSQLAdapter()
	if err != nil {
		return nil, fmt.Errorf("failed to create base PostgreSQL adapter: %v", err)
	}

	// Create adapter
	adapter := &AivenPostgreSQLAdapter{
		DefaultPostgreSQLAdapter: *defaultAdapter,
		aivenConfig:              aivenConfig,
		serviceHandler:           serviceHandler,
		state: &adeptersinterfaces.AivenState{
			LastAppliedConfig: time.Time{},
		},
	}

	// Initialize collectors
	adapter.MetricsState = agent.MetricsState{
		Collectors: AivenCollectors(adapter),
		Cache:      agent.Caches{},
		Mutex:      &sync.Mutex{},
	}

	return adapter, nil
}

// Interface implementation methods
func (adapter *AivenPostgreSQLAdapter) GetAivenState() *adeptersinterfaces.AivenState {
	return adapter.state
}

func (adapter *AivenPostgreSQLAdapter) GetAivenConfig() *adeptersinterfaces.AivenConfig {
	return &adapter.aivenConfig
}

func (adapter *AivenPostgreSQLAdapter) PGDriver() *pgPool.Pool {
	return adapter.pgDriver
}

func (adapter *AivenPostgreSQLAdapter) APIClient() *retryablehttp.Client {
	return adapter.ApiClient
}

// getServicePlanResourceInfo retrieves the CPU and memory information for a service plan
// directly from the Aiven API and database by querying runtime information
func getServicePlanResourceInfo(ctx context.Context, adapter *AivenPostgreSQLAdapter) (int, int64, error) {
	// Get service information from Aiven API
	service, err := adapter.serviceHandler.ServiceGet(
		ctx,
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get service info from API: %v", err)
	}
	
	// Try to get CPU count from PostgreSQL
	var cpuCount int
	err = adapter.pgDriver.QueryRow(ctx, "SELECT current_setting('max_worker_processes')::int").Scan(&cpuCount)
	if err != nil {
		adapter.Logger().Warnf("Failed to get CPU count from PostgreSQL: %v", err)
		
		// Use node_count from service if available
		if service.NodeCount != nil && *service.NodeCount > 0 {
			cpuCount = *service.NodeCount * 2 // Estimate: typically each node has 2 CPUs
		} else {
			// Use a safe default
			cpuCount = 2
			adapter.Logger().Warnf("Could not determine CPU count, using default: %d", cpuCount)
		}
	}
	
	// Try to get memory information from PostgreSQL
	var totalMemoryMB int64
	err = adapter.pgDriver.QueryRow(ctx, "SELECT ROUND(current_setting('shared_buffers')::bigint*8/1024/0.25)").Scan(&totalMemoryMB)
	if err != nil {
		adapter.Logger().Warnf("Failed to get memory from PostgreSQL: %v", err)
		
		// Fallback to node_memory_mb if available
		if service.NodeMemoryMb != nil && *service.NodeMemoryMb > 0 {
			totalMemoryMB = int64(*service.NodeMemoryMb)
		} else {
			// Use a safe default
			totalMemoryMB = 4096 // 4 GB in MB
			adapter.Logger().Warnf("Could not determine memory size, using default: %d MB", totalMemoryMB)
		}
	}
	
	// Convert MB to bytes
	totalMemoryBytes := totalMemoryMB * 1024 * 1024
	
	return cpuCount, totalMemoryBytes, nil
}

// GetSystemInfo returns system information for the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) GetSystemInfo() ([]utils.FlatValue, error) {
	adapter.Logger().Info("Collecting Aiven system info")

	var systemInfo []utils.FlatValue
	ctx := context.Background()

	// Get service information from Aiven API
	service, err := adapter.serviceHandler.ServiceGet(
		ctx,
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get service info: %v", err)
	}

	// Get accurate CPU and memory information
	numCPUs, totalMemoryBytes, err := getServicePlanResourceInfo(ctx, adapter)
	if err != nil {
		adapter.Logger().Warnf("Error getting resource info: %v", err)
		// Continue with collection, we'll try to get other metrics
	}

	// Store hardware information in state
	adapter.state.Hardware = &adeptersinterfaces.AivenHardwareState{
		TotalMemoryBytes: totalMemoryBytes,
		NumCPUs:          numCPUs,
		LastChecked:      time.Now(),
	}

	// Get PostgreSQL version and max connections from database
	pgVersion, err := collectors.PGVersion(adapter)
	if err != nil {
		return nil, err
	}

	maxConnections, err := collectors.MaxConnections(adapter)
	if err != nil {
		return nil, err
	}

	// Create metrics
	totalMemory, _ := utils.NewMetric("node_memory_total", totalMemoryBytes, utils.Int)
	noCPUsMetric, _ := utils.NewMetric("node_cpu_count", numCPUs, utils.Int)
	version, _ := utils.NewMetric("system_info_pg_version", pgVersion, utils.String)
	maxConnectionsMetric, _ := utils.NewMetric("pg_max_connections", maxConnections, utils.Int)
	planMetric, _ := utils.NewMetric("aiven_service_plan", service.Plan, utils.String)
	
	// Get service state
	stateMetric, _ := utils.NewMetric("aiven_service_state", service.State, utils.String)
	
	// Aiven uses SSD storage
	diskTypeMetric, _ := utils.NewMetric("node_disk_device_type", "SSD", utils.String)
	
	// Get disk information if available from service info
	if service.DiskSpaceMb != nil && *service.DiskSpaceMb > 0 {
		diskSizeMB := *service.DiskSpaceMb
		diskSizeBytes := diskSizeMB * 1024 * 1024
		diskSizeMetric, _ := utils.NewMetric("node_disk_size", diskSizeBytes, utils.Int)
		systemInfo = append(systemInfo, diskSizeMetric)
	}
	
	// Try to get used disk space from PostgreSQL
	var usedDiskBytes int64
	err = adapter.pgDriver.QueryRow(ctx, "SELECT COALESCE(SUM(pg_database_size(datname)), 0) FROM pg_database").Scan(&usedDiskBytes)
	if err == nil {
		diskUsedMetric, _ := utils.NewMetric("node_disk_used", usedDiskBytes, utils.Int)
		systemInfo = append(systemInfo, diskUsedMetric)
		
		// Calculate disk usage percentage if we have total disk space
		if service.DiskSpaceMb != nil && *service.DiskSpaceMb > 0 {
			diskTotalBytes := int64(*service.DiskSpaceMb) * 1024 * 1024
			diskPercentage := float64(usedDiskBytes) * 100.0 / float64(diskTotalBytes)
			diskPercentMetric, _ := utils.NewMetric("node_disk_usage_percent", diskPercentage, utils.Float)
			systemInfo = append(systemInfo, diskPercentMetric)
		}
	}

	systemInfo = append(systemInfo, version, totalMemory, maxConnectionsMetric, noCPUsMetric, 
		diskTypeMetric, planMetric, stateMetric)

	return systemInfo, nil
}

// ApplyConfig applies configuration changes to the Aiven PostgreSQL service
func (adapter *AivenPostgreSQLAdapter) ApplyConfig(proposedConfig *agent.ProposedConfigResponse) error {
	adapter.Logger().Infof("Applying Config to Aiven PostgreSQL: %s", proposedConfig.KnobApplication)

	// If the last applied config is less than 1 minute ago, return
	if adapter.state.LastAppliedConfig.Add(1 * time.Minute).After(time.Now()) {
		adapter.Logger().Info("Last applied config is less than 1 minute ago, skipping")
		return nil
	}

	// Get the current service configuration
	service, err := adapter.serviceHandler.ServiceGet(
		context.Background(),
		adapter.aivenConfig.ProjectName, 
		adapter.aivenConfig.ServiceName,
		service.ServiceGetIncludeSecrets(true), // Include secrets to get current user_config
	)
	if err != nil {
		return fmt.Errorf("failed to get service configuration: %v", err)
	}

	// Start with current user_config or create a new one
	userConfig := service.UserConfig
	if userConfig == nil {
		userConfig = make(map[string]any)
	}

	// Ensure pg configuration section exists
	pgParams, ok := userConfig["pg"]
	if !ok || pgParams == nil {
		pgParams = make(map[string]any)
	}

	pgParamsMap, ok := pgParams.(map[string]any)
	if !ok {
		pgParamsMap = make(map[string]any)
	}

	// Track if any changes were made
	changesMade := false

	// Apply the proposed config changes
	for _, knob := range proposedConfig.KnobsOverrides {
		knobConfig, err := parameters.FindRecommendedKnob(proposedConfig.Config, knob)
		if err != nil {
			return fmt.Errorf("failed to find recommended knob: %v", err)
		}
		
		// Handle shared_buffers specially - convert to shared_buffers_percentage if needed
		paramName := knobConfig.Name
		if paramName == "shared_buffers" {
			// Convert shared_buffers to shared_buffers_percentage
			if adapter.state.Hardware != nil && adapter.state.Hardware.TotalMemoryBytes > 0 {
				// Shared buffers in pg is in 8KB blocks, convert to bytes
				sharedBuffersBytes := float64(knobConfig.Setting.(float64)) * 8 * 1024
				// Calculate as percentage of total memory
				percentage := (sharedBuffersBytes / float64(adapter.state.Hardware.TotalMemoryBytes)) * 100
				// Ensure within valid range for Aiven (20-60%)
				if percentage < 20 {
					percentage = 20
				} else if percentage > 60 {
					percentage = 60
				}
				paramName = "shared_buffers_percentage"
				knobConfig.Setting = percentage
				adapter.Logger().Infof("Converting shared_buffers to shared_buffers_percentage: %.2f%%", percentage)
			} else {
				adapter.Logger().Warn("Cannot convert shared_buffers to percentage - hardware info not available")
				continue
			}
		}
		
		// Check if parameter is known to be modifiable
		paramInfo, exists := aivenModifiableParams[paramName]
		if !exists {
			adapter.Logger().Warnf("Parameter %s has unknown modifiability status on Aiven, attempting anyway", paramName)
			// Default to applying under pg config
			paramInfo = struct {
				Modifiable  bool
				ServiceLevel bool
			}{true, false}
		} else if !paramInfo.Modifiable {
			adapter.Logger().Warnf("Parameter %s is known to be restricted by Aiven, skipping", paramName)
			continue
		}
		
		// Special handling for max_worker_processes - can only be increased
		if paramName == "max_worker_processes" {
			// Get current value from service
			var currentValue int
			if pgCurrentValue, ok := pgParamsMap[paramName]; ok {
				currentValue = int(pgCurrentValue.(float64))
			} else {
				// Try at service level
				if svcCurrentValue, ok := userConfig[paramName]; ok {
					currentValue = int(svcCurrentValue.(float64))
				}
			}
			
			// Check if new value is lower than current
			newValue := int(knobConfig.Setting.(float64))
			if newValue < currentValue {
				adapter.Logger().Warnf("Cannot decrease max_worker_processes from %d to %d on Aiven", currentValue, newValue)
				continue
			}
		}
		
		// Convert setting to appropriate type
		var settingValue any
		switch v := knobConfig.Setting.(type) {
		case float64:
			// For numeric parameters that should be integer, convert to integer format
			if v == float64(int64(v)) {
				settingValue = int64(v)
			} else {
				settingValue = v
			}
		case bool:
			settingValue = v
		case string:
			settingValue = v
		default:
			// For other types, convert to string
			settingValue = fmt.Sprintf("%v", v)
		}
		
		// Apply the parameter at the correct level (service level or pg config)
		if paramInfo.ServiceLevel {
			userConfig[paramName] = settingValue
		} else {
			pgParamsMap[paramName] = settingValue
		}
		
		changesMade = true
	}

	// Only update if changes were made
	if !changesMade {
		adapter.Logger().Info("No applicable changes to apply")
		return nil
	}

	// Update the user_config with modified pg parameters
	userConfig["pg"] = pgParamsMap
	
	// Create the update request
	serviceUpdateIn := &service.ServiceUpdateIn{
		UserConfig: &userConfig,
	}

	// For parameters that require restart, we need to power cycle the service
	needsRestart := proposedConfig.KnobApplication == "restart"
	
	// Apply the configuration update
	_, err = adapter.serviceHandler.ServiceUpdate(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
		serviceUpdateIn,
	)
	if err != nil {
		return fmt.Errorf("failed to update PostgreSQL parameters: %v", err)
	}

	// If restart is required, power-cycle the service
	if needsRestart {
		adapter.Logger().Info("Restarting Aiven PostgreSQL service")
        
		// First, power down the service by setting powered: false
		powerDownUpdate := &service.ServiceUpdateIn{
			Powered: boolPtr(false),
		}
		
		_, err = adapter.serviceHandler.ServiceUpdate(
			context.Background(),
			adapter.aivenConfig.ProjectName,
			adapter.aivenConfig.ServiceName,
			powerDownUpdate,
		)
		if err != nil {
			return fmt.Errorf("failed to power down service: %v", err)
		}

		// Wait for the service to be powered down
		err = adapter.waitForServiceState("POWEROFF")
		if err != nil {
			return err
		}

		// Power the service back up
		powerUpUpdate := &service.ServiceUpdateIn{
			Powered: boolPtr(true),
		}
		
		_, err = adapter.serviceHandler.ServiceUpdate(
			context.Background(),
			adapter.aivenConfig.ProjectName,
			adapter.aivenConfig.ServiceName,
			powerUpUpdate,
		)
		if err != nil {
			return fmt.Errorf("failed to power up service: %v", err)
		}
	}

	// Wait for the service to be running again
	err = adapter.waitForServiceState("RUNNING")
	if err != nil {
		return err
	}

	// Verify PostgreSQL is responding
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL to come back online")
		case <-time.After(5 * time.Second):
			_, err := adapter.pgDriver.Exec(ctx, "SELECT 1")
			if err == nil {
				adapter.Logger().Info("PostgreSQL is back online")
				adapter.state.LastAppliedConfig = time.Now()
				return nil
			}
			adapter.Logger().Debug("PostgreSQL not ready yet, retrying...")
		}
	}
}

// waitForServiceState waits for the service to reach the specified state
func (adapter *AivenPostgreSQLAdapter) waitForServiceState(targetState string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for service to reach state %s", targetState)
		case <-time.After(10 * time.Second):
			service, err := adapter.serviceHandler.ServiceGet(
				context.Background(),
				adapter.aivenConfig.ProjectName,
				adapter.aivenConfig.ServiceName,
			)
			if err != nil {
				adapter.Logger().Warnf("Failed to get service status: %v", err)
				continue
			}

			if service.State == targetState {
				adapter.Logger().Infof("Service reached state: %s", targetState)
				return nil
			}
			adapter.Logger().Debugf("Service state: %s, waiting for: %s", service.State, targetState)
		}
	}
}

// Guardrails implements resource usage guardrails
func (adapter *AivenPostgreSQLAdapter) Guardrails() *agent.GuardrailType {
	if time.Since(adapter.state.LastGuardrailCheck) < 5*time.Second {
		return nil
	}
	
	adapter.Logger().Info("Checking guardrails for Aiven PostgreSQL")

	if adapter.state.Hardware == nil {
		adapter.Logger().Warn("Hardware information not available, skipping guardrails")
		return nil
	}

	adapter.state.LastGuardrailCheck = time.Now()

	// Query database size for memory usage approximation
	var memoryUsage int64
	err := adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT COALESCE(sum(pg_database_size(datname)), 0) FROM pg_database`).Scan(&memoryUsage)
	
	if err != nil {
		// On error, try just the current database
		adapter.Logger().Warnf("Failed to get full memory usage, falling back to current database: %v", err)
		err = adapter.pgDriver.QueryRow(context.Background(), 
			`SELECT pg_database_size(current_database())`).Scan(&memoryUsage)
		
		if err != nil {
			adapter.Logger().Errorf("Failed to get memory usage: %v", err)
			return nil
		}
	}

	// Get connection count for CPU usage approximation
	var connectionCount int
	err = adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle'`).Scan(&connectionCount)
	
	if err != nil {
		adapter.Logger().Errorf("Failed to get connection count: %v", err)
		connectionCount = 0
	}

	// Get max connections
	var maxConnections int
	err = adapter.pgDriver.QueryRow(context.Background(), 
		`SELECT current_setting('max_connections')::int`).Scan(&maxConnections)
	
	if err != nil {
		adapter.Logger().Errorf("Failed to get max connections: %v", err)
		maxConnections = 100 // Default
	}

	// Calculate usage percentages
	memoryUsagePercent := (float64(memoryUsage) / float64(adapter.state.Hardware.TotalMemoryBytes)) * 100
	connectionUsagePercent := (float64(connectionCount) / float64(maxConnections)) * 100

	adapter.Logger().Infof("Memory usage: %.2f%%, Connection usage: %.2f%%", memoryUsagePercent, connectionUsagePercent)
	
	// If memory usage is over 90% or connection usage is over 90%, trigger critical guardrail
	if memoryUsagePercent > 90 || connectionUsagePercent > 90 {
		critical := agent.Critical
		return &critical
	}
	
	// Check for service health issues from the API
	serviceHealth, err := adapter.checkServiceHealth()
	if err != nil {
		adapter.Logger().Warnf("Failed to check service health from API: %v", err)
	} else if !serviceHealth {
		// If we detected a service issue from the API, trigger critical guardrail
		adapter.Logger().Warn("Service health check failed from API")
		critical := agent.Critical
		return &critical
	}

	return nil
}

// checkServiceHealth checks if the service is healthy using the Aiven API
func (adapter *AivenPostgreSQLAdapter) checkServiceHealth() (bool, error) {
	service, err := adapter.serviceHandler.ServiceGet(
		context.Background(),
		adapter.aivenConfig.ProjectName,
		adapter.aivenConfig.ServiceName,
	)
	if err != nil {
		return false, fmt.Errorf("failed to get service: %v", err)
	}
	
	// Check if service is in a healthy state
	// RUNNING is the normal state for an Aiven service
	if service.State != "RUNNING" {
		return false, nil
	}
	
	return true, nil
}

// AivenCollectors returns the metrics collectors for Aiven PostgreSQL
func AivenCollectors(adapter *AivenPostgreSQLAdapter) []agent.MetricCollector {
	return []agent.MetricCollector{
		{
			Key:        "database_average_query_runtime",
			MetricType: "float",
			Collector:  collectors.AivenQueryRuntime(adapter), // Use Aiven-specific collector
		},
		{
			Key:        "database_transactions_per_second",
			MetricType: "int",
			Collector:  collectors.TransactionsPerSecond(adapter),
		},
		{
			Key:        "database_active_connections",
			MetricType: "int",
			Collector:  collectors.ActiveConnections(adapter),
		},
		{
			Key:        "system_db_size",
			MetricType: "int",
			Collector:  collectors.DatabaseSize(adapter),
		},
		{
			Key:        "database_autovacuum_count",
			MetricType: "int",
			Collector:  collectors.Autovacuum(adapter),
		},
		{
			Key:        "server_uptime",
			MetricType: "float",
			Collector:  collectors.Uptime(adapter),
		},
		{
			Key:        "database_cache_hit_ratio",
			MetricType: "float",
			Collector:  collectors.BufferCacheHitRatio(adapter),
		},
		{
			Key:        "database_wait_events",
			MetricType: "int",
			Collector:  collectors.WaitEvents(adapter),
		},
		{
			Key:        "hardware",
			MetricType: "int",
			Collector:  collectors.AivenHardwareInfo(adapter),
		},
	}
}

// AivenHardwareInfo collects hardware metrics for Aiven PostgreSQL
func AivenHardwareInfo(adapter adeptersinterfaces.AivenPostgreSQLAdapter) func(ctx context.Context, state *agent.MetricsState) error {
	return func(ctx context.Context, state *agent.MetricsState) error {
		// For connection count as CPU usage indicator
		var connectionCount int
		err := adapter.PGDriver().QueryRow(ctx, 
			`SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle'`).Scan(&connectionCount)
		
		if err != nil {
			adapter.Logger().Warnf("Failed to get connection count: %v", err)
			connectionCount = 0
		}

		// Get max connections
		var maxConnections int
		err = adapter.PGDriver().QueryRow(ctx, 
			`SELECT current_setting('max_connections')::int`).Scan(&maxConnections)
		
		if err != nil {
			adapter.Logger().Warnf("Failed to get max connections: %v", err)
			maxConnections = 100 // Default
		}

		// Calculate CPU usage as connection percentage
		cpuUsage := (float64(connectionCount) / float64(maxConnections)) * 100
		cpuUsageMetric, _ := utils.NewMetric("node_cpu_usage", cpuUsage, utils.Float)
		state.AddMetric(cpuUsageMetric)

		// For memory used, check database size
		var memoryUsed int64
		err = adapter.PGDriver().QueryRow(ctx, 
			`SELECT COALESCE(sum(pg_database_size(datname)), 0) FROM pg_database`).Scan(&memoryUsed)
		
		if err != nil {
			// On error, try just the current database
			adapter.Logger().Warnf("Failed to get full memory usage, falling back to current database: %v", err)
			err = adapter.PGDriver().QueryRow(ctx, 
				`SELECT pg_database_size(current_database())`).Scan(&memoryUsed)
			
			if err != nil {
				adapter.Logger().Warnf("Failed to get memory usage: %v", err)
				memoryUsed = 0
			}
		}
		
		memoryUsedMetric, _ := utils.NewMetric("node_memory_used", memoryUsed, utils.Int)
		state.AddMetric(memoryUsedMetric)

		// For memory available, calculate from total minus used
		if adapter.GetAivenState().Hardware != nil {
			memoryAvailable := adapter.GetAivenState().Hardware.TotalMemoryBytes - memoryUsed
			if memoryAvailable < 0 {
				memoryAvailable = 0
			}
			
			memoryAvailableMetric, _ := utils.NewMetric("node_memory_available", memoryAvailable, utils.Int)
			state.AddMetric(memoryAvailableMetric)
		}

		// Add connection usage percentage
		connectionUsageMetric, _ := utils.NewMetric("node_connection_usage_percent", cpuUsage, utils.Float)
		state.AddMetric(connectionUsageMetric)
		
		// Get service information via API if possible
		if aivenAdapter, ok := adapter.(adeptersinterfaces.AivenPostgreSQLAdapter); ok {
			service, err := adapter.GetAivenConfig().GetService(ctx)
			if err == nil && service != nil {
				// Add service state as metric
				stateMetric, _ := utils.NewMetric("aiven_service_state", service.State, utils.String)
				state.AddMetric(stateMetric)
				
				// Add plan as metric
				planMetric, _ := utils.NewMetric("aiven_service_plan", service.Plan, utils.String)
				state.AddMetric(planMetric)
				
				// Add disk space metrics if available
				if service.DiskSpaceMb != nil && *service.DiskSpaceMb > 0 {
					diskSizeMB := int64(*service.DiskSpaceMb)
					diskSizeBytes := diskSizeMB * 1024 * 1024
					diskTotalMetric, _ := utils.NewMetric("node_disk_total", diskSizeBytes, utils.Int)
					state.AddMetric(diskTotalMetric)
					
					// Calculate disk usage percentage if we have both total and used
					if memoryUsed > 0 {
						diskPercentage := (float64(memoryUsed) / float64(diskSizeBytes)) * 100
						diskUsageMetric, _ := utils.NewMetric("node_disk_usage_percent", diskPercentage, utils.Float)
						state.AddMetric(diskUsageMetric)
					}
				}
			}
		}

		return nil
	}
}
