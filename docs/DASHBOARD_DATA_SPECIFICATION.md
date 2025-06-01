# Dashboard Data Specification

## Overview

This document defines the data structures, formats, and specifications required for the NextJS Traffic Monitoring Dashboard. It ensures data consistency, optimal visualization, and real-time responsiveness for traffic management operations.

## Data Flow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Raw Simulator  │───▶│   Processed     │───▶│   Dashboard     │───▶│   UI Components │
│      Data       │    │   Backend Data  │    │   Optimized     │    │                 │
│                 │    │                 │    │      Data       │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Data Models

### 1. Enhanced Traffic Data Model

**Purpose**: Primary traffic metrics for dashboard visualization
**Update Frequency**: Every 5 seconds
**Retention**: 7 days in cache, 30 days in database

```typescript
interface EnhancedTrafficData {
  // Core Identification
  sensor_id: string;
  intersection_id: string;
  sensor_direction: 'north' | 'south' | 'east' | 'west';
  timestamp: string; // ISO 8601 format
  
  // Location Data
  location: {
    id: string;
    name: string;
    coordinates: {
      lat: number;
      lng: number;
    };
    zone_id: string;
  };
  
  // Traffic Metrics
  traffic_metrics: {
    vehicle_count: number;
    average_speed: number; // km/h
    traffic_density: number; // vehicles per km
    congestion_level: 'low' | 'medium' | 'high' | 'critical';
    flow_rate: number; // vehicles per minute
    occupancy_rate: number; // percentage
  };
  
  // Vehicle Distribution
  vehicle_types: {
    cars: number;
    trucks: number;
    buses: number;
    motorcycles: number;
    bicycles: number;
    pedestrians: number;
  };
  
  // Environmental Conditions (Coordinated)
  environment: {
    weather: 'sunny' | 'rain' | 'snow' | 'fog';
    temperature: number; // Celsius
    humidity: number; // percentage
    visibility: 'good' | 'fair' | 'poor';
    road_condition: 'dry' | 'wet' | 'icy' | 'flooded';
    air_quality_index: number;
  };
  
  // Traffic Management
  traffic_control: {
    light_status: 'green' | 'yellow' | 'red';
    light_phase_remaining: number; // seconds
    pedestrian_crossing: boolean;
    emergency_override: boolean;
  };
  
  // Incident Information
  incidents: {
    active_incidents: number;
    incident_severity: 'none' | 'minor' | 'major' | 'critical';
    estimated_delay: number; // minutes
    affected_lanes: number;
  };
  
  // Quality Metrics
  data_quality: {
    confidence_score: number; // 0-1
    sensor_health: 'healthy' | 'degraded' | 'critical';
    last_calibration: string;
    message_count: number;
  };
}
```

### 2. Intersection Summary Model

**Purpose**: Aggregated intersection-wide metrics
**Update Frequency**: Every 3 seconds
**Retention**: 24 hours in cache, 7 days in database

```typescript
interface IntersectionSummary {
  // Identification
  intersection_id: string;
  intersection_name: string;
  timestamp: string;
  
  // Geographic Information
  coordinates: {
    center: { lat: number; lng: number };
    bounds: {
      north: number;
      south: number;
      east: number;
      west: number;
    };
  };
  
  // Aggregate Traffic Metrics
  traffic_summary: {
    total_vehicles: number;
    vehicles_per_hour: number;
    average_speed: number;
    peak_congestion_level: 'low' | 'medium' | 'high' | 'critical';
    throughput_efficiency: number; // percentage
  };
  
  // Direction-Specific Data
  directional_flow: {
    north: DirectionalMetrics;
    south: DirectionalMetrics;
    east: DirectionalMetrics;
    west: DirectionalMetrics;
  };
  
  // Traffic Light Coordination
  traffic_lights: {
    current_phase: 'north_south_green' | 'east_west_green' | 'all_red';
    phase_duration: number; // seconds
    cycle_time: number; // total cycle duration
    efficiency_score: number; // 0-1
    pedestrian_phase_active: boolean;
  };
  
  // Queue Analysis
  queue_analysis: {
    max_queue_length: number;
    average_wait_time: number;
    queue_by_direction: {
      north: number;
      south: number;
      east: number;
      west: number;
    };
  };
  
  // Performance Indicators
  performance: {
    level_of_service: 'A' | 'B' | 'C' | 'D' | 'E' | 'F';
    delay_index: number;
    capacity_utilization: number; // percentage
    incident_impact_score: number; // 0-10
  };
  
  // Environmental Impact
  environmental_impact: {
    estimated_emissions: number; // CO2 equivalent
    noise_level: number; // decibels
    fuel_consumption: number; // liters per hour
  };
}

interface DirectionalMetrics {
  vehicle_count: number;
  average_speed: number;
  queue_length: number;
  turning_movements: {
    straight: number;
    left: number;
    right: number;
    u_turn: number;
  };
}
```

### 3. Real-Time Alert Model

**Purpose**: Incident and anomaly notifications
**Update Frequency**: Immediate (event-driven)
**Retention**: 30 days

```typescript
interface TrafficAlert {
  // Alert Identification
  alert_id: string;
  alert_type: AlertType;
  severity: 'info' | 'warning' | 'critical' | 'emergency';
  timestamp: string;
  
  // Location Information
  location: {
    intersection_id?: string;
    sensor_id?: string;
    coordinates: { lat: number; lng: number };
    address: string;
    affected_area: 'single_sensor' | 'intersection' | 'corridor' | 'zone';
  };
  
  // Alert Details
  details: {
    title: string;
    description: string;
    duration_estimate: number; // minutes
    confidence_level: number; // 0-1
    auto_detected: boolean;
  };
  
  // Impact Assessment
  impact: {
    affected_vehicles: number;
    delay_minutes: number;
    alternative_routes: string[];
    traffic_disruption_level: number; // 1-10
  };
  
  // Response Information
  response: {
    status: 'new' | 'acknowledged' | 'in_progress' | 'resolved';
    assigned_to?: string;
    estimated_resolution: string;
    actions_taken: string[];
  };
  
  // Related Data
  related_sensors: string[];
  historical_pattern: boolean;
  weather_related: boolean;
}

type AlertType = 
  | 'traffic_jam'
  | 'accident'
  | 'road_closure'
  | 'weather_hazard'
  | 'wrong_way_driver'
  | 'pedestrian_incident'
  | 'sensor_malfunction'
  | 'traffic_light_failure'
  | 'emergency_vehicle'
  | 'construction'
  | 'special_event';
```

### 4. Historical Analytics Model

**Purpose**: Trend analysis and reporting
**Update Frequency**: Hourly aggregation
**Retention**: 1 year

```typescript
interface TrafficAnalytics {
  // Time Period
  time_period: {
    start_time: string;
    end_time: string;
    granularity: 'minute' | 'hour' | 'day' | 'week' | 'month';
  };
  
  // Location Scope
  scope: {
    type: 'sensor' | 'intersection' | 'corridor' | 'city';
    identifiers: string[];
    geographic_bounds?: {
      north: number;
      south: number;
      east: number;
      west: number;
    };
  };
  
  // Traffic Patterns
  patterns: {
    peak_hours: {
      morning: { start: string; end: string; intensity: number };
      evening: { start: string; end: string; intensity: number };
      weekend?: { start: string; end: string; intensity: number };
    };
    
    daily_trends: Array<{
      hour: number;
      average_volume: number;
      average_speed: number;
      congestion_probability: number;
    }>;
    
    weekly_trends: Array<{
      day_of_week: number; // 0-6
      traffic_intensity: number;
      incident_frequency: number;
    }>;
  };
  
  // Performance Metrics
  performance_metrics: {
    average_travel_time: number;
    reliability_index: number; // 0-1
    congestion_hours_per_day: number;
    incident_rate: number; // incidents per day
    level_of_service_distribution: {
      A: number; B: number; C: number; D: number; E: number; F: number;
    };
  };
  
  // Comparative Analysis
  comparisons: {
    vs_previous_period: {
      volume_change: number; // percentage
      speed_change: number; // percentage
      incident_change: number; // percentage
    };
    
    vs_seasonal_average: {
      volume_variance: number;
      pattern_similarity: number; // 0-1
    };
  };
  
  // Predictions
  predictions: {
    next_hour_forecast: {
      expected_volume: number;
      confidence_interval: [number, number];
      congestion_probability: number;
    };
    
    daily_forecast: Array<{
      hour: number;
      volume_forecast: number;
      congestion_forecast: 'low' | 'medium' | 'high';
    }>;
  };
}
```

## Dashboard Component Data Requirements

### 1. Real-Time Traffic Map

**Data Sources**: EnhancedTrafficData, IntersectionSummary, TrafficAlert
**Update Frequency**: 2-5 seconds
**Performance Requirements**: <100ms render time

```typescript
interface MapData {
  sensors: Array<{
    id: string;
    coordinates: { lat: number; lng: number };
    status: SensorStatus;
    current_metrics: {
      speed: number;
      volume: number;
      congestion: 'low' | 'medium' | 'high' | 'critical';
    };
    direction: 'north' | 'south' | 'east' | 'west';
  }>;
  
  intersections: Array<{
    id: string;
    coordinates: { lat: number; lng: number };
    traffic_light_status: TrafficLightPhase;
    congestion_level: number; // 0-10
    incident_active: boolean;
  }>;
  
  alerts: Array<{
    id: string;
    coordinates: { lat: number; lng: number };
    type: AlertType;
    severity: 'info' | 'warning' | 'critical' | 'emergency';
    radius: number; // affected area in meters
  }>;
  
  traffic_flows: Array<{
    from_sensor: string;
    to_sensor: string;
    flow_rate: number;
    speed: number;
    path_coordinates: Array<{ lat: number; lng: number }>;
  }>;
}
```

### 2. Intersection Widget

**Data Sources**: IntersectionSummary, EnhancedTrafficData
**Update Frequency**: 3 seconds
**Performance Requirements**: Smooth animations, <50ms state updates

```typescript
interface IntersectionWidgetData {
  intersection_info: {
    id: string;
    name: string;
    coordinates: { lat: number; lng: number };
  };
  
  current_state: {
    traffic_light_phase: TrafficLightPhase;
    phase_time_remaining: number;
    total_vehicles: number;
    throughput_rate: number; // vehicles per minute
  };
  
  directional_data: {
    north: DirectionalData;
    south: DirectionalData;
    east: DirectionalData;
    west: DirectionalData;
  };
  
  performance_indicators: {
    efficiency_score: number; // 0-100
    average_wait_time: number;
    level_of_service: 'A' | 'B' | 'C' | 'D' | 'E' | 'F';
    incident_impact: boolean;
  };
  
  trends: {
    volume_trend: 'increasing' | 'decreasing' | 'stable';
    speed_trend: 'increasing' | 'decreasing' | 'stable';
    congestion_trend: 'improving' | 'worsening' | 'stable';
  };
}

interface DirectionalData {
  lane_count: number;
  current_vehicles: number;
  queue_length: number;
  average_speed: number;
  sensor_id: string;
  turning_movements: {
    straight: number;
    left: number;
    right: number;
  };
}
```

### 3. Analytics Dashboard

**Data Sources**: TrafficAnalytics, Historical Data
**Update Frequency**: 1 minute for live data, on-demand for historical
**Performance Requirements**: <500ms for chart rendering

```typescript
interface AnalyticsDashboardData {
  overview_metrics: {
    total_daily_volume: number;
    average_speed: number;
    peak_congestion_level: number;
    incident_count: number;
    efficiency_score: number;
  };
  
  time_series_data: {
    traffic_volume: Array<{
      timestamp: string;
      value: number;
      sensor_id?: string;
    }>;
    
    speed_data: Array<{
      timestamp: string;
      average_speed: number;
      congestion_level: number;
    }>;
    
    incident_timeline: Array<{
      timestamp: string;
      incident_type: AlertType;
      duration: number;
      impact_score: number;
    }>;
  };
  
  comparative_analysis: {
    daily_comparison: {
      today: number;
      yesterday: number;
      week_ago: number;
      change_percentage: number;
    };
    
    seasonal_patterns: Array<{
      period: string;
      average_volume: number;
      peak_times: string[];
    }>;
  };
  
  predictive_insights: {
    next_hour_forecast: {
      volume: number;
      congestion_probability: number;
      recommended_actions: string[];
    };
    
    pattern_anomalies: Array<{
      description: string;
      severity: 'low' | 'medium' | 'high';
      recommendation: string;
    }>;
  };
}
```

## Data Consistency Requirements

### 1. Spatial Consistency

**Weather Data Correlation**:
- Sensors within 500m radius must report identical weather conditions
- Weather updates propagate with realistic timing (5-15 minute delays for weather changes)
- Seasonal patterns align with geographic location

**Traffic Flow Conservation**:
- Vehicle counts between adjacent sensors must balance within 5% tolerance
- Traffic speeds show logical correlation based on distance and time
- Congestion propagates realistically between connected sensors

### 2. Temporal Consistency

**Timestamp Synchronization**:
- All data timestamps use UTC format: `YYYY-MM-DDTHH:mm:ss.sssZ`
- Maximum timestamp variance between correlated events: 1 second
- Data ordering maintained in real-time streams

**Update Frequencies**:
```typescript
const UPDATE_FREQUENCIES = {
  raw_vehicle_data: 750, // milliseconds
  traffic_data: 5000, // 5 seconds
  intersection_data: 3000, // 3 seconds
  sensor_health: 60000, // 1 minute
  traffic_alerts: 0, // immediate
  analytics_data: 3600000, // 1 hour
} as const;
```

### 3. Data Validation Rules

**Real-Time Validation**:
```typescript
interface ValidationRules {
  speed_limits: {
    min: 0;
    max: 120; // km/h
    urban_max: 60;
    highway_max: 120;
  };
  
  vehicle_counts: {
    min: 0;
    max_per_lane: 50;
    max_intersection: 200;
  };
  
  temporal_validation: {
    max_age_seconds: 300; // 5 minutes
    future_tolerance_seconds: 10;
  };
  
  spatial_validation: {
    max_distance_for_correlation: 500; // meters
    speed_change_tolerance: 20; // km/h between adjacent sensors
  };
}
```

## WebSocket Event Specifications

### 1. Real-Time Events

```typescript
// Client → Server
interface ClientEvents {
  'subscribe-intersection': (intersectionId: string) => void;
  'subscribe-alerts': () => void;
  'subscribe-analytics': (config: AnalyticsConfig) => void;
  'unsubscribe-intersection': (intersectionId: string) => void;
}

// Server → Client
interface ServerEvents {
  'traffic-update': (data: EnhancedTrafficData) => void;
  'intersection-update': (data: IntersectionSummary) => void;
  'alert': (alert: TrafficAlert) => void;
  'sensor-status': (status: SensorHealthUpdate) => void;
  'analytics-update': (data: TrafficAnalytics) => void;
  'connection-status': (status: ConnectionStatus) => void;
}
```

### 2. Data Compression

For high-frequency updates, implement data compression:

```typescript
interface CompressedUpdate {
  sensor_id: string;
  timestamp: number; // Unix timestamp
  deltas: {
    speed?: number; // Only if changed > 5 km/h
    volume?: number; // Only if changed > 2 vehicles
    congestion?: number; // Only if level changed
  };
  full_update?: boolean; // True every 30 seconds
}
```

## Performance Specifications

### 1. Response Time Requirements

| Component | Target | Maximum |
|-----------|--------|---------|
| Real-time data update | <100ms | 200ms |
| Chart rendering | <200ms | 500ms |
| Map updates | <150ms | 300ms |
| Historical data query | <1s | 3s |
| Alert notifications | <50ms | 100ms |

### 2. Data Volume Limits

```typescript
interface DataLimits {
  max_sensors_per_dashboard: 50;
  max_alerts_displayed: 100;
  max_historical_days: 365;
  max_concurrent_connections: 1000;
  max_real_time_updates_per_second: 100;
}
```

### 3. Cache Strategy

```typescript
interface CacheConfiguration {
  real_time_data: {
    ttl: 300; // 5 minutes
    max_entries: 10000;
  };
  
  intersection_summaries: {
    ttl: 60; // 1 minute
    max_entries: 1000;
  };
  
  historical_analytics: {
    ttl: 3600; // 1 hour
    max_entries: 500;
  };
  
  static_configuration: {
    ttl: 86400; // 24 hours
    max_entries: 100;
  };
}
```

## Error Handling and Fallbacks

### 1. Data Quality Indicators

```typescript
interface DataQualityMetrics {
  completeness: number; // 0-1, percentage of expected data received
  timeliness: number; // 0-1, data freshness score
  accuracy: number; // 0-1, validation pass rate
  consistency: number; // 0-1, cross-sensor correlation score
}
```

### 2. Fallback Strategies

```typescript
interface FallbackStrategies {
  missing_sensor_data: 'interpolate' | 'use_last_known' | 'mark_unavailable';
  network_disconnection: 'cached_data' | 'degraded_mode' | 'offline_message';
  invalid_data: 'discard' | 'sanitize' | 'flag_as_uncertain';
  high_latency: 'reduce_frequency' | 'prioritize_critical' | 'compress_updates';
}
```

This specification ensures the NextJS dashboard receives consistent, high-quality, and real-time traffic data optimized for visualization and decision-making, Big Daddy. 