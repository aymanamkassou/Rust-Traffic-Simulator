#![warn(keyword_idents_2024)]
use chrono::{DateTime, Datelike, Timelike, Utc};
use futures::future::join_all;
use rand::{
    rngs::StdRng,
    seq::SliceRandom,
    Rng, SeedableRng,
};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, time};
use uuid::Uuid;
use std::collections::HashMap;

// ===== Intersection Controller Structures =====

#[derive(Debug, Clone)]
pub struct IntersectionController {
    intersection_id: String,
    sensors: Vec<String>,
    traffic_light_cycle: TrafficLightCycle,
    shared_weather_state: WeatherState,
    base_traffic_density: f32,
    vehicle_flow_tracker: VehicleFlowTracker,
    // NEW FIELDS for realistic weather
    last_weather_update: Instant,
    weather_change_interval_minutes: u64, // Weather changes every 15-30 minutes
    weather_transition_target: Option<WeatherState>, // Target weather for gradual transition
    weather_transition_progress: f32, // 0.0 to 1.0
}

#[derive(Debug, Clone)]
pub struct TrafficLightCycle {
    current_phase: TrafficPhase,
    phase_start_time: Instant,
    cycle_duration_s: u32,
}

#[derive(Debug, Clone)]
pub enum TrafficPhase {
    NorthSouthGreen,   // N,S sensors get green, E,W get red
    NorthSouthYellow,  // N,S sensors get yellow, E,W stay red
    EastWestGreen,     // E,W sensors get green, N,S get red  
    EastWestYellow,    // E,W sensors get yellow, N,S stay red
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherState {
    conditions: String,
    temperature: f32,
    humidity: u16,
    wind_speed: u16,
    visibility: String,
    road_condition: String,
}

#[derive(Debug, Clone)]
pub struct VehicleFlowTracker {
    flow_rates: std::collections::HashMap<String, f32>, // sensor_id -> vehicles per minute
    queue_propagation: std::collections::HashMap<String, f32>, // sensor_id -> propagation factor
}

// ===== Data Structures =====

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VehicleRecord {
    id: String,
    sensor_id: String,
    timestamp: DateTime<Utc>,
    speed_kmh: f32,
    length_dm: u16,
    vehicle_class: String,
    occupancy_s: f32,
    time_gap_s: f32,
    status: u8,
    counter: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SensorHealth {
    sensor_id: String,
    timestamp: DateTime<Utc>,
    battery_level: f32,
    temperature_c: f32,
    hw_fault: bool,
    low_voltage: bool,
    uptime_s: u64,
    message_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrafficData {
    sensor_id: String,
    timestamp: DateTime<Utc>,
    location_id: String,
    location_x: f32,
    location_y: f32,
    density: u16,
    travel_time: u16,
    vehicle_number: u16,
    speed: u16,
    direction_change: String,
    pedestrian_count: u16,
    bicycle_count: u16,
    heavy_vehicle_count: u16,
    incident_detected: bool,
    visibility: String,
    weather_conditions: String,
    road_condition: String,
    congestion_level: String,
    average_vehicle_size: String,
    vehicle_type_distribution: VehicleTypeDistribution,
    traffic_flow_direction: String,
    red_light_violations: u16,
    temperature: f32,
    humidity: u16,
    wind_speed: u16,
    air_quality_index: u16,
    near_miss_events: u16,
    accident_severity: String,
    roadwork_detected: bool,
    illegal_parking_cases: u16,
    // NEW FIELDS (backwards compatible)
    intersection_id: String,
    sensor_direction: String, // "north", "south", "east", "west"
    coordinated_weather: WeatherState, // Now managed by intersection controller
    traffic_light_phase: String, // Coordinated across intersection
    vehicle_flow_rate: f32, // Vehicles per minute flowing through
    queue_propagation_factor: f32, // How congestion spreads
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VehicleTypeDistribution {
    cars: u16,
    buses: u16,
    motorcycles: u16,
    trucks: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntersectionData {
    sensor_id: String,
    timestamp: DateTime<Utc>,
    intersection_id: String,
    stopped_vehicles_count: u16,
    average_wait_time: u16,
    left_turn_count: u16,
    right_turn_count: u16,
    average_speed_by_direction: AverageSpeedByDirection,
    lane_occupancy: u16,
    intersection_blocking_vehicles: u16,
    traffic_light_compliance_rate: u16,
    pedestrians_crossing: u16,
    jaywalking_pedestrians: u16,
    cyclists_crossing: u16,
    risky_behavior_detected: bool,
    queue_length_by_lane: QueueLengthByLane,
    intersection_congestion_level: String,
    intersection_crossing_time: u16,
    traffic_light_impact: String,
    near_miss_incidents: u16,
    collision_count: u16,
    sudden_braking_events: u16,
    illegal_parking_detected: bool,
    wrong_way_vehicles: u16,
    ambient_light_level: u16,
    traffic_light_status: String,
    local_weather_conditions: String,
    fog_or_smoke_detected: bool,
    // NEW FIELDS (backwards compatible)
    coordinated_light_status: String, // "north_south_green" | "east_west_green"
    phase_time_remaining: u16, // Seconds until next phase
    intersection_efficiency: f32, // Overall throughput efficiency
    total_intersection_vehicles: u16, // Sum across all 4 sensors
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AverageSpeedByDirection {
    north_south: u16,
    east_west: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueLengthByLane {
    lane1: u16,
    lane2: u16,
    lane3: u16,
}

struct TrafficSimulator {
    sensor_id: String,
    location_id: String,
    location_x: f32,
    location_y: f32,
    intersection_id: String,
    producer: FutureProducer,
    start_time: Instant,
    message_count: u64,
    rng: StdRng,
    sensor_direction: String, // NEW FIELD: "north", "south", "east", "west"
}

// ===== Intersection Controller Implementation =====

impl IntersectionController {
    fn new(intersection_id: String, sensors: Vec<String>) -> Self {
        IntersectionController {
            intersection_id,
            sensors,
            traffic_light_cycle: TrafficLightCycle {
                current_phase: TrafficPhase::NorthSouthGreen,
                phase_start_time: Instant::now(),
                cycle_duration_s: 90, // 90 second phases
            },
            shared_weather_state: WeatherState {
                conditions: "sunny".to_string(),
                temperature: 20.0,
                humidity: 50,
                wind_speed: 10,
                visibility: "good".to_string(),
                road_condition: "dry".to_string(),
            },
            base_traffic_density: 30.0,
            vehicle_flow_tracker: VehicleFlowTracker {
                flow_rates: std::collections::HashMap::new(),
                queue_propagation: std::collections::HashMap::new(),
            },
            // NEW FIELDS for realistic weather
            last_weather_update: Instant::now(),
            weather_change_interval_minutes: 15, // Weather changes every 15-30 minutes
            weather_transition_target: None, // Target weather for gradual transition
            weather_transition_progress: 0.0, // 0.0 to 1.0
        }
    }

    fn update_shared_weather(&mut self) {
        // Check if it's time for a weather change (every 15-30 minutes)
        let elapsed_minutes = self.last_weather_update.elapsed().as_secs() / 60;
        
        // Only consider weather changes every 15-30 minutes
        if elapsed_minutes >= self.weather_change_interval_minutes {
            // Decide if weather should change (only 30% chance)
            let mut rng = rand::thread_rng();
            if rng.gen_bool(0.3) {
                self.initiate_weather_transition();
                self.last_weather_update = Instant::now();
                // Set next interval (15-30 minutes)
                self.weather_change_interval_minutes = rng.gen_range(15..=30);
            }
        }
        
        // Apply gradual weather transitions if in progress
        self.apply_weather_transition();
    }
    
    fn initiate_weather_transition(&mut self) {
        let hour = Utc::now().hour();
        let month = Utc::now().month();
        let mut rng = rand::thread_rng();
        
        // Generate target weather based on current conditions and realistic transitions
        let current_conditions = &self.shared_weather_state.conditions;
        
        // Realistic weather transitions (weather doesn't jump from sunny to snow)
        let possible_transitions = match current_conditions.as_str() {
            "sunny" => vec!["cloudy", "partly_cloudy"],
            "partly_cloudy" => vec!["sunny", "cloudy", "overcast"],
            "cloudy" => vec!["partly_cloudy", "overcast", "light_rain"],
            "overcast" => vec!["cloudy", "light_rain", "fog"],
            "light_rain" => vec!["overcast", "rain", "cloudy"],
            "rain" => vec!["light_rain", "overcast", "heavy_rain"],
            "heavy_rain" => vec!["rain", "storm"],
            "storm" => vec!["heavy_rain", "rain"],
            "fog" => vec!["overcast", "cloudy"],
            "snow" => vec!["overcast", "light_snow"],
            "light_snow" => vec!["snow", "overcast"],
            _ => vec!["sunny", "cloudy"],
        };
        
        // Add seasonal weather bias
        let seasonal_weather = match month {
            12 | 1 | 2 => vec!["snow", "light_snow", "overcast"], // Winter
            3 | 4 | 5 => vec!["light_rain", "rain", "cloudy"],    // Spring  
            6 | 7 | 8 => vec!["sunny", "partly_cloudy"],          // Summer
            _ => vec!["overcast", "cloudy", "light_rain"],        // Fall
        };
        
        // Combine current transitions with seasonal bias
        let mut weighted_options = possible_transitions.clone();
        weighted_options.extend(seasonal_weather);
        
        let target_conditions = weighted_options.choose(&mut rng).unwrap_or(&"sunny").to_string();
        
        // Calculate realistic target temperature (gradual change)
        let current_temp = self.shared_weather_state.temperature;
        let temp_change = rng.gen_range(-3.0..=3.0); // Max 3°C change
        let mut target_temp = current_temp + temp_change;
        
        // Apply seasonal temperature constraints
        let (min_temp, max_temp) = match month {
            12 | 1 | 2 => (-15.0, 10.0),  // Winter
            3 | 4 | 5 => (0.0, 25.0),     // Spring
            6 | 7 | 8 => (15.0, 40.0),    // Summer
            _ => (5.0, 30.0),             // Fall
        };
        target_temp = target_temp.clamp(min_temp, max_temp);
        
        // Calculate realistic humidity change
        let current_humidity = self.shared_weather_state.humidity;
        let humidity_change = rng.gen_range(-10..=10);
        let target_humidity = (current_humidity as i16 + humidity_change).clamp(20, 95) as u16;
        
        // Calculate realistic wind speed change
        let current_wind = self.shared_weather_state.wind_speed;
        let wind_change = rng.gen_range(-5..=5);
        let target_wind = (current_wind as i16 + wind_change).clamp(0, 50) as u16;
        
        // Set visibility and road condition based on target weather
        let (target_visibility, target_road_condition) = match target_conditions.as_str() {
            "fog" => ("poor", "wet"),
            "rain" | "heavy_rain" | "storm" => ("fair", "wet"),
            "light_rain" => ("good", "wet"),
            "snow" | "light_snow" => ("poor", if rng.gen_bool(0.8) { "icy" } else { "wet" }),
            _ => ("good", "dry"),
        };
        
        self.weather_transition_target = Some(WeatherState {
            conditions: target_conditions,
            temperature: target_temp,
            humidity: target_humidity,
            wind_speed: target_wind,
            visibility: target_visibility.to_string(),
            road_condition: target_road_condition.to_string(),
        });
        
        self.weather_transition_progress = 0.0;
    }
    
    fn apply_weather_transition(&mut self) {
        if let Some(target) = &self.weather_transition_target {
            // Transition happens over 10 minutes (600 seconds)
            let transition_duration_seconds = 600.0;
            let progress_increment = 1.0 / transition_duration_seconds; // Per second
            
            self.weather_transition_progress += progress_increment;
            
            if self.weather_transition_progress >= 1.0 {
                // Transition complete
                self.shared_weather_state = target.clone();
                self.weather_transition_target = None;
                self.weather_transition_progress = 0.0;
            } else {
                // Interpolate between current and target weather
                let progress = self.weather_transition_progress;
                
                // Interpolate temperature
                let current_temp = self.shared_weather_state.temperature;
                let target_temp = target.temperature;
                self.shared_weather_state.temperature = current_temp + (target_temp - current_temp) * progress;
                
                // Interpolate humidity
                let current_humidity = self.shared_weather_state.humidity as f32;
                let target_humidity = target.humidity as f32;
                self.shared_weather_state.humidity = (current_humidity + (target_humidity - current_humidity) * progress) as u16;
                
                // Interpolate wind speed
                let current_wind = self.shared_weather_state.wind_speed as f32;
                let target_wind = target.wind_speed as f32;
                self.shared_weather_state.wind_speed = (current_wind + (target_wind - current_wind) * progress) as u16;
                
                // Switch discrete values at 50% progress
                if progress >= 0.5 {
                    self.shared_weather_state.conditions = target.conditions.clone();
                    self.shared_weather_state.visibility = target.visibility.clone();
                    self.shared_weather_state.road_condition = target.road_condition.clone();
                }
            }
        }
    }
    
    fn get_weather_for_sensor(&self, _sensor_id: &str) -> WeatherState {
        // All sensors return the same weather state
        self.shared_weather_state.clone()
    }
    
    fn update_traffic_lights(&mut self) {
        let elapsed = self.traffic_light_cycle.phase_start_time.elapsed();
        let phase_duration = Duration::from_secs(self.traffic_light_cycle.cycle_duration_s as u64);
        
        if elapsed >= phase_duration {
            // Move to next phase
            self.traffic_light_cycle.current_phase = match self.traffic_light_cycle.current_phase {
                TrafficPhase::NorthSouthGreen => TrafficPhase::NorthSouthYellow,
                TrafficPhase::NorthSouthYellow => TrafficPhase::EastWestGreen,
                TrafficPhase::EastWestGreen => TrafficPhase::EastWestYellow,
                TrafficPhase::EastWestYellow => TrafficPhase::NorthSouthGreen,
            };
            self.traffic_light_cycle.phase_start_time = Instant::now();
        }
    }
    
    fn get_light_status_for_sensor(&self, sensor_direction: &str) -> String {
        match (&self.traffic_light_cycle.current_phase, sensor_direction) {
            (TrafficPhase::NorthSouthGreen, "north") | (TrafficPhase::NorthSouthGreen, "south") => "green",
            (TrafficPhase::NorthSouthYellow, "north") | (TrafficPhase::NorthSouthYellow, "south") => "yellow",
            (TrafficPhase::EastWestGreen, "east") | (TrafficPhase::EastWestGreen, "west") => "green",
            (TrafficPhase::EastWestYellow, "east") | (TrafficPhase::EastWestYellow, "west") => "yellow",
            _ => "red",
        }.to_string()
    }
    
    fn get_coordinated_light_status(&self) -> String {
        match self.traffic_light_cycle.current_phase {
            TrafficPhase::NorthSouthGreen | TrafficPhase::NorthSouthYellow => "north_south_green",
            TrafficPhase::EastWestGreen | TrafficPhase::EastWestYellow => "east_west_green",
        }.to_string()
    }
    
    fn get_phase_time_remaining(&self) -> u16 {
        let elapsed = self.traffic_light_cycle.phase_start_time.elapsed();
        let phase_duration = Duration::from_secs(self.traffic_light_cycle.cycle_duration_s as u64);
        
        if elapsed < phase_duration {
            (phase_duration - elapsed).as_secs() as u16
        } else {
            0
        }
    }
    
    fn update_vehicle_flow(&mut self, sensor_id: &str, vehicle_count: u16, density: u16) {
        // Calculate vehicles per minute flow rate
        let flow_rate = vehicle_count as f32 * 12.0; // Assuming 5-second updates, so *12 for per minute
        
        // Calculate queue propagation based on density
        let propagation_factor = if density > 70 {
            0.8 // High congestion spreads quickly
        } else if density > 40 {
            0.5 // Medium congestion moderate spread
        } else {
            0.2 // Low congestion minimal spread
        };
        
        self.vehicle_flow_tracker.flow_rates.insert(sensor_id.to_string(), flow_rate);
        self.vehicle_flow_tracker.queue_propagation.insert(sensor_id.to_string(), propagation_factor);
    }
    
    fn get_vehicle_flow_rate(&self, sensor_id: &str) -> f32 {
        self.vehicle_flow_tracker.flow_rates.get(sensor_id).copied().unwrap_or(0.0)
    }
    
    fn get_queue_propagation_factor(&self, sensor_id: &str) -> f32 {
        self.vehicle_flow_tracker.queue_propagation.get(sensor_id).copied().unwrap_or(0.0)
    }
    
    fn calculate_intersection_efficiency(&self) -> f32 {
        // Calculate efficiency based on flow rates and light coordination
        let total_flow: f32 = self.vehicle_flow_tracker.flow_rates.values().sum();
        let avg_flow = if !self.vehicle_flow_tracker.flow_rates.is_empty() {
            total_flow / self.vehicle_flow_tracker.flow_rates.len() as f32
        } else {
            0.0
        };
        
        // Efficiency is higher when flow is balanced and coordinated
        let max_theoretical_flow = 120.0; // vehicles per minute per sensor
        (avg_flow / max_theoretical_flow).min(1.0)
    }
    
    fn get_total_intersection_vehicles(&self) -> u16 {
        // Sum flow rates across all sensors (simplified calculation)
        self.vehicle_flow_tracker.flow_rates.values().sum::<f32>() as u16 / 12 // Convert back to current count
    }
}

// Helper function for weighted choice
fn weighted_choice<T: Clone>(options: &[T], weights: &[u32], rng: &mut impl Rng) -> T {
    assert_eq!(options.len(), weights.len());

    let total: u32 = weights.iter().sum();
    let mut rnd = rng.gen_range(0..total);

    for (i, &weight) in weights.iter().enumerate() {
        if rnd < weight {
            return options[i].clone();
        }
        rnd -= weight;
    }

    // Fallback
    options[0].clone()
}

// ===== Vehicle Simulator Logic =====

impl TrafficSimulator {
    fn new(
        sensor_id: &str,
        location_id: &str,
        location_x: f32,
        location_y: f32,
        intersection_id: &str,
        sensor_direction: &str,
        kafka_brokers: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Configure Kafka producer
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(TrafficSimulator {
            sensor_id: sensor_id.to_string(),
            location_id: location_id.to_string(),
            location_x,
            location_y,
            intersection_id: intersection_id.to_string(),
            producer,
            start_time: Instant::now(),
            message_count: 0,
            rng: StdRng::from_entropy(),
            sensor_direction: sensor_direction.to_string(),
        })
    }

    async fn generate_vehicle_data(&mut self) -> Result<VehicleRecord, Box<dyn std::error::Error>> {
        // Generate realistic vehicle data based on time of day patterns
        let hour = Utc::now().hour();

        // Traffic patterns vary by time of day - adjust parameters for realism
        let (min_speed, max_speed) = match hour {
            6..=9 => (5, 40),    // Morning rush hour
            10..=15 => (20, 70), // Midday
            16..=19 => (5, 50),  // Evening rush hour
            _ => (30, 90),       // Night time
        };

        // Vehicle class distribution also varies by time
        let vehicle_classes = vec![
            "passenger_car",
            "suv",
            "pickup_truck",
            "motorcycle",
            "bus",
            "semi_truck",
            "delivery_van",
        ];
        let vehicle_class_weights = match hour {
            6..=8 => vec![70, 15, 5, 2, 5, 2, 1], // More buses in morning commute
            16..=19 => vec![65, 20, 8, 2, 2, 1, 2], // More cars/SUVs in evening
            9..=15 => vec![50, 20, 10, 5, 2, 8, 5], // More delivery vehicles midday
            _ => vec![60, 15, 10, 10, 1, 3, 1],   // Night mix
        };

        // Generate vehicle length based on class
        let vehicle_class = self.weighted_choice(&vehicle_classes, &vehicle_class_weights);
        let length_dm = match vehicle_class {
            "passenger_car" => self.rng.gen_range(30..45),
            "suv" => self.rng.gen_range(45..55),
            "pickup_truck" => self.rng.gen_range(50..65),
            "motorcycle" => self.rng.gen_range(15..25),
            "bus" => self.rng.gen_range(100..140),
            "semi_truck" => self.rng.gen_range(150..220),
            "delivery_van" => self.rng.gen_range(55..75),
            _ => self.rng.gen_range(30..60),
        };

        // Generate speed with slight noise for realism
        let speed_base = self.rng.gen_range(min_speed..max_speed);
        let speed_noise = (self.rng.r#gen::<f32>() - 0.5) * 5.0;
        let speed_kmh = (speed_base as f32 + speed_noise).max(0.0);

        // Calculate realistic time gap based on traffic density
        let time_gap_s = if hour >= 6 && hour <= 9 || hour >= 16 && hour <= 19 {
            // Rush hour - smaller gaps
            self.rng.gen_range(0.5..5.0)
        } else {
            // Normal traffic - larger gaps
            self.rng.gen_range(2.0..15.0)
        };

        // Calculate occupancy based on vehicle length and speed
        let occupancy_s = if speed_kmh > 0.0 {
            (length_dm as f32 / 10.0) / (speed_kmh / 3.6)
        } else {
            0.0
        };

        // Status byte contains sensor health and traffic condition flags
        let wrong_way = self.rng.gen_bool(0.01); // 1% chance of wrong-way driver
        let queue_detected =
            self.rng
                .gen_bool(if hour >= 7 && hour <= 9 || hour >= 16 && hour <= 19 {
                    0.3 // 30% chance during rush hour
                } else {
                    0.05 // 5% chance otherwise
                });

        let hw_fault = self.rng.gen_bool(0.001); // 0.1% chance of hardware fault
        let low_voltage = self.rng.gen_bool(0.005); // 0.5% chance of low voltage warning

        let mut status: u8 = 0;
        if hw_fault {
            status |= 0x04;
        }
        if low_voltage {
            status |= 0x08;
        }
        if wrong_way {
            status |= 0x10;
        }
        if queue_detected {
            status |= 0x20;
        }

        self.message_count += 1;

        let vehicle_record = VehicleRecord {
            id: Uuid::new_v4().to_string(),
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            speed_kmh,
            length_dm,
            vehicle_class: vehicle_class.to_string(),
            occupancy_s,
            time_gap_s,
            status,
            counter: self.message_count as u32,
        };

        // Send to Kafka
        let payload = serde_json::to_string(&vehicle_record)?;
        self.producer
            .send(
                FutureRecord::to("raw-vehicle-data")
                    .payload(&payload)
                    .key(&vehicle_record.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        // Check for special conditions and send alerts
        if wrong_way {
            let alert = serde_json::json!({
                "type": "wrong-way-driver",
                "timestamp": Utc::now(),
                "sensor_id": self.sensor_id,
                "vehicle_data": vehicle_record
            });

            self.producer
                .send(
                    FutureRecord::to("traffic-alerts")
                        .payload(&alert.to_string())
                        .key(&self.sensor_id),
                    Timeout::After(Duration::from_secs(0)),
                )
                .await
                .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;
        }

        if queue_detected {
            let alert = serde_json::json!({
                "type": "traffic-queue",
                "timestamp": Utc::now(),
                "sensor_id": self.sensor_id,
                "vehicle_data": vehicle_record
            });

            self.producer
                .send(
                    FutureRecord::to("traffic-alerts")
                        .payload(&alert.to_string())
                        .key(&self.sensor_id),
                    Timeout::After(Duration::from_secs(0)),
                )
                .await
                .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;
        }

        Ok(vehicle_record)
    }

    async fn generate_traffic_data(&mut self) -> Result<TrafficData, Box<dyn std::error::Error>> {
        let hour = Utc::now().hour();

        // Time-based traffic modeling for more realistic data
        let density_base = match hour {
            6..=9 => 50..95,   // Morning rush
            10..=15 => 30..70, // Daytime
            16..=19 => 60..95, // Evening rush
            _ => 5..40,        // Night
        };

        // Generate traffic data
        let density = self.rng.gen_range(density_base);

        let travel_time = match density {
            0..=30 => self.rng.gen_range(5..15),   // Low traffic
            31..=70 => self.rng.gen_range(15..30), // Medium traffic
            _ => self.rng.gen_range(30..60),       // Heavy traffic
        };

        let weather_options = ["sunny", "rain", "snow", "fog"];
        let weather_weights = match Utc::now().month() {
            12 | 1 | 2 => vec![40, 20, 35, 5], // Winter - more snow
            3 | 4 | 5 => vec![50, 40, 5, 5],   // Spring - more rain
            6 | 7 | 8 => vec![80, 15, 0, 5],   // Summer - mostly sunny
            _ => vec![60, 30, 5, 5],           // Fall - mixed
        };
        let weather_conditions = self
            .weighted_choice(&weather_options, &weather_weights)
            .to_string();

        // Road condition correlates with weather
        let road_condition = match weather_conditions.as_str() {
            "sunny" => "dry",
            "rain" => "wet",
            "snow" => {
                if self.rng.gen_bool(0.7) {
                    "icy"
                } else {
                    "wet"
                }
            }
            "fog" => {
                if self.rng.gen_bool(0.3) {
                    "wet"
                } else {
                    "dry"
                }
            }
            _ => "dry",
        };

        // Environmental parameters
        let temperature = match Utc::now().month() {
            12 | 1 | 2 => self.rng.gen_range(-10.0..5.0), // Winter
            3 | 4 | 5 => self.rng.gen_range(5.0..20.0),   // Spring
            6 | 7 | 8 => self.rng.gen_range(20.0..35.0),  // Summer
            _ => self.rng.gen_range(5.0..25.0),           // Fall
        };

        // Calculate vehicle numbers based on density
        let vehicle_base = density * 2;
        let vehicle_number = vehicle_base + self.rng.gen_range(0..20);

        // Vehicle type distribution is time-dependent
        let car_ratio = match hour {
            6..=8 | 16..=19 => 0.7, // Rush hour - more cars
            9..=15 => 0.65,         // Business hours - more trucks/deliveries
            _ => 0.75,              // Night - mostly cars
        };

        let cars = (vehicle_number as f32 * car_ratio) as u16;
        let buses = if hour >= 6 && hour <= 9 || hour >= 15 && hour <= 19 {
            self.rng.gen_range(3..10) // More buses during commute hours
        } else {
            self.rng.gen_range(0..5)
        };
        let trucks = if hour >= 9 && hour <= 17 {
            self.rng.gen_range(5..15) // More trucks during business hours
        } else {
            self.rng.gen_range(0..8)
        };
        let motorcycles = (vehicle_number as f32 * 0.05) as u16 + self.rng.gen_range(0..10);

        // Speed correlates with density
        let speed = match density {
            0..=30 => self.rng.gen_range(50..80), // Low traffic - higher speeds
            31..=70 => self.rng.gen_range(30..60), // Medium traffic
            _ => self.rng.gen_range(5..40),       // Heavy traffic - lower speeds
        };

        // Congestion level correlates directly with density
        let congestion_level = match density {
            0..=30 => "low",
            31..=70 => "medium",
            _ => "high",
        };

        let traffic_data = TrafficData {
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            location_id: self.location_id.clone(),
            location_x: self.location_x,
            location_y: self.location_y,
            density,
            travel_time,
            vehicle_number,
            speed,
            direction_change: self.random_choice(&["left", "right", "none"]).to_string(),
            pedestrian_count: self.rng.gen_range(0..50),
            bicycle_count: self.rng.gen_range(0..20),
            heavy_vehicle_count: trucks,
            incident_detected: self.rng.gen_bool(0.1), // 10% chance of incident
            visibility: self.random_choice(&["good", "fair", "poor"]).to_string(),
            weather_conditions,
            road_condition: road_condition.to_string(),
            congestion_level: congestion_level.to_string(),
            average_vehicle_size: self
                .random_choice(&["small", "medium", "large"])
                .to_string(),
            vehicle_type_distribution: VehicleTypeDistribution {
                cars,
                buses,
                motorcycles,
                trucks,
            },
            traffic_flow_direction: self
                .random_choice(&["north-south", "east-west", "both"])
                .to_string(),
            red_light_violations: self.rng.gen_range(0..5),
            temperature,
            humidity: self.rng.gen_range(0..100),
            wind_speed: self.rng.gen_range(0..40),
            air_quality_index: self.rng.gen_range(0..500),
            near_miss_events: self.rng.gen_range(0..5),
            accident_severity: self.random_choice(&["none", "minor", "major"]).to_string(),
            roadwork_detected: self.rng.gen_bool(0.1),
            illegal_parking_cases: self.rng.gen_range(0..10),
            intersection_id: self.intersection_id.clone(),
            sensor_direction: self.sensor_direction.clone(),
            coordinated_weather: self.get_weather_for_sensor(&self.sensor_id),
            traffic_light_phase: self.get_light_status_for_sensor(&self.sensor_direction),
            vehicle_flow_rate: self.get_vehicle_flow_rate(&self.sensor_id),
            queue_propagation_factor: self.get_queue_propagation_factor(&self.sensor_id),
        };

        // Send to Kafka
        let payload = serde_json::to_string(&traffic_data)?;
        self.producer
            .send(
                FutureRecord::to("traffic-data")
                    .payload(&payload)
                    .key(&traffic_data.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        Ok(traffic_data)
    }

    async fn generate_intersection_data(
        &mut self,
    ) -> Result<IntersectionData, Box<dyn std::error::Error>> {
        let hour = Utc::now().hour();

        // Time-based intersection modeling
        let is_rush_hour = hour >= 7 && hour <= 9 || hour >= 16 && hour <= 19;
        let is_business_hours = hour >= 9 && hour <= 17;

        // Generate realistic queue sizes based on time of day
        let queue_base = if is_rush_hour {
            self.rng.gen_range(5..20)
        } else if is_business_hours {
            self.rng.gen_range(2..15)
        } else {
            self.rng.gen_range(0..10)
        };

        let lane1_queue = queue_base + self.rng.gen_range(0..5);
        let lane2_queue = queue_base + self.rng.gen_range(0..5);
        let lane3_queue = queue_base + self.rng.gen_range(0..5);

        // Generate wait times correlated with queue length
        let avg_wait_time = match (lane1_queue + lane2_queue + lane3_queue) / 3 {
            0..=5 => self.rng.gen_range(5..30),
            6..=15 => self.rng.gen_range(30..60),
            _ => self.rng.gen_range(60..120),
        };

        // Traffic light status - simulate cycle
        let seconds_in_day =
            (Utc::now().hour() * 3600 + Utc::now().minute() * 60 + Utc::now().second()) % 180;
        let traffic_light_status = match seconds_in_day % 180 {
            0..=90 => "green",
            91..=120 => "yellow",
            _ => "red",
        };

        // Generate intersection congestion correlated with queues
        let intersection_congestion_level = match (lane1_queue + lane2_queue + lane3_queue) / 3 {
            0..=5 => "low",
            6..=15 => "medium",
            _ => "high",
        };

        // Generate speeds by direction
        let ns_speed = self.rng.gen_range(20..60);
        let ew_speed = self.rng.gen_range(20..60);

        // Pedestrian activity correlates with time of day
        let pedestrians_crossing = if is_business_hours {
            self.rng.gen_range(5..40)
        } else {
            self.rng.gen_range(0..20)
        };

        // Generate intersection data
        let intersection_data = IntersectionData {
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            intersection_id: self.intersection_id.clone(),
            stopped_vehicles_count: lane1_queue + lane2_queue + lane3_queue,
            average_wait_time: avg_wait_time,
            left_turn_count: self.rng.gen_range(0..30),
            right_turn_count: self.rng.gen_range(0..30),
            average_speed_by_direction: AverageSpeedByDirection {
                north_south: ns_speed,
                east_west: ew_speed,
            },
            lane_occupancy: self.rng.gen_range(0..100),
            intersection_blocking_vehicles: if intersection_congestion_level == "high" {
                self.rng.gen_range(0..5)
            } else {
                self.rng.gen_range(0..2)
            },
            traffic_light_compliance_rate: self.rng.gen_range(70..100),
            pedestrians_crossing,
            jaywalking_pedestrians: (pedestrians_crossing as f32 * 0.2) as u16,
            cyclists_crossing: self.rng.gen_range(0..15),
            risky_behavior_detected: self.rng.gen_bool(0.2),
            queue_length_by_lane: QueueLengthByLane {
                lane1: lane1_queue,
                lane2: lane2_queue,
                lane3: lane3_queue,
            },
            intersection_congestion_level: intersection_congestion_level.to_string(),
            intersection_crossing_time: if intersection_congestion_level == "high" {
                self.rng.gen_range(60..120)
            } else {
                self.rng.gen_range(10..60)
            },
            traffic_light_impact: self.random_choice(&["low", "moderate", "high"]).to_string(),
            near_miss_incidents: self.rng.gen_range(0..5),
            collision_count: if self.rng.gen_bool(0.05) {
                self.rng.gen_range(1..3)
            } else {
                0
            },
            sudden_braking_events: self.rng.gen_range(0..10),
            illegal_parking_detected: self.rng.gen_bool(0.2),
            wrong_way_vehicles: if self.rng.gen_bool(0.05) { 1 } else { 0 },
            ambient_light_level: match hour {
                6..=8 => self.rng.gen_range(50..150),   // Dawn
                9..=17 => self.rng.gen_range(150..200), // Day
                18..=20 => self.rng.gen_range(50..150), // Dusk
                _ => self.rng.gen_range(0..50),         // Night
            },
            traffic_light_status: traffic_light_status.to_string(),
            local_weather_conditions: self
                .random_choice(&["clear", "rain", "snow", "fog"])
                .to_string(),
            fog_or_smoke_detected: self.rng.gen_bool(0.15),
            coordinated_light_status: self.get_coordinated_light_status(),
            phase_time_remaining: self.get_phase_time_remaining() as u16,
            intersection_efficiency: self.calculate_intersection_efficiency(),
            total_intersection_vehicles: self.get_total_intersection_vehicles() as u16,
        };

        // Send to Kafka
        let payload = serde_json::to_string(&intersection_data)?;
        self.producer
            .send(
                FutureRecord::to("intersection-data")
                    .payload(&payload)
                    .key(&intersection_data.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        Ok(intersection_data)
    }

    async fn send_health_data(&mut self) -> Result<SensorHealth, Box<dyn std::error::Error>> {
        // Simulate sensor health metrics
        let battery_level = 100.0 - (self.start_time.elapsed().as_secs() as f32 / 36000.0); // Simulate battery drain
        let temperature_c = 25.0 + (self.rng.r#gen::<f32>() - 0.5) * 5.0; // Temperature fluctuation

        let hw_fault = self.rng.gen_bool(0.001); // 0.1% chance
        let low_voltage = battery_level < 20.0 || self.rng.gen_bool(0.005); // Low battery or 0.5% random chance

        let health = SensorHealth {
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            battery_level,
            temperature_c,
            hw_fault,
            low_voltage,
            uptime_s: self.start_time.elapsed().as_secs(),
            message_count: self.message_count,
        };

        // Send to Kafka
        let health_json = serde_json::to_string(&health)?;
        self.producer
            .send(
                FutureRecord::to("sensor-health")
                    .payload(&health_json)
                    .key(&self.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        Ok(health)
    }

    // Utility function to pick a random choice
    fn random_choice<T: Clone>(&mut self, options: &[T]) -> T {
        options.choose(&mut self.rng).unwrap().clone()
    }

    // Utility function for weighted random choice
    fn weighted_choice<T: Clone>(&mut self, options: &[T], weights: &[u32]) -> T {
        assert_eq!(options.len(), weights.len());

        let total: u32 = weights.iter().sum();
        let mut rnd = self.rng.gen_range(0..total);

        for (i, &weight) in weights.iter().enumerate() {
            if rnd < weight {
                return options[i].clone();
            }
            rnd -= weight;
        }

        // Fallback
        options[0].clone()
    }

    // Add method to get coordinated weather from intersection controller
    fn get_weather_for_sensor(&self, _sensor_id: &str) -> WeatherState {
        // This will be called from the intersection controller
        // For now, generate basic weather - will be overridden by controller
        WeatherState {
            conditions: "sunny".to_string(),
            temperature: 20.0,
            humidity: 50,
            wind_speed: 10,
            visibility: "good".to_string(),
            road_condition: "dry".to_string(),
        }
    }

    // Add method to get coordinated traffic light status
    fn get_light_status_for_sensor(&self, _sensor_direction: &str) -> String {
        // This will be called from the intersection controller
        // For now, return basic status - will be overridden by controller
        "green".to_string()
    }

    // Add method to get vehicle flow rate
    fn get_vehicle_flow_rate(&self, _sensor_id: &str) -> f32 {
        // This will be calculated by the intersection controller
        0.0
    }

    // Add method to get queue propagation factor
    fn get_queue_propagation_factor(&self, _sensor_id: &str) -> f32 {
        // This will be calculated by the intersection controller
        0.0
    }

    // Add method to get coordinated light status
    fn get_coordinated_light_status(&self) -> String {
        // This will be called from the intersection controller
        "north_south_green".to_string()
    }

    // Add method to get phase time remaining
    fn get_phase_time_remaining(&self) -> u16 {
        // This will be called from the intersection controller
        45
    }

    // Add method to calculate intersection efficiency
    fn calculate_intersection_efficiency(&self) -> f32 {
        // This will be calculated by the intersection controller
        0.75
    }

    // Add method to get total intersection vehicles
    fn get_total_intersection_vehicles(&self) -> u16 {
        // This will be calculated by the intersection controller
        20
    }

    // Add enhanced method for traffic data generation with controller
    async fn generate_traffic_data_with_controller(
        &mut self,
        controller: Arc<Mutex<IntersectionController>>,
    ) -> Result<TrafficData, Box<dyn std::error::Error>> {
        let hour = Utc::now().hour();

        // Time-based traffic modeling for more realistic data
        let density_base = match hour {
            6..=9 => 50..95,   // Morning rush
            10..=15 => 30..70, // Daytime
            16..=19 => 60..95, // Evening rush
            _ => 5..40,        // Night
        };

        // Generate traffic data
        let density = self.rng.gen_range(density_base);

        let travel_time = match density {
            0..=30 => self.rng.gen_range(5..15),   // Low traffic
            31..=70 => self.rng.gen_range(15..30), // Medium traffic
            _ => self.rng.gen_range(30..60),       // Heavy traffic
        };

        // Get coordinated weather from intersection controller
        let controller_guard = controller.lock().await;
        let coordinated_weather = controller_guard.get_weather_for_sensor(&self.sensor_id);
        let traffic_light_phase = controller_guard.get_light_status_for_sensor(&self.sensor_direction);
        let vehicle_flow_rate = controller_guard.get_vehicle_flow_rate(&self.sensor_id);
        let queue_propagation_factor = controller_guard.get_queue_propagation_factor(&self.sensor_id);
        drop(controller_guard);

        // Road condition correlates with weather
        let road_condition = &coordinated_weather.road_condition;

        // Calculate vehicle numbers based on density
        let vehicle_base = density * 2;
        let vehicle_number = vehicle_base + self.rng.gen_range(0..20);

        // Vehicle type distribution is time-dependent
        let car_ratio = match hour {
            6..=8 | 16..=19 => 0.7, // Rush hour - more cars
            9..=15 => 0.65,         // Business hours - more trucks/deliveries
            _ => 0.75,              // Night - mostly cars
        };

        let cars = (vehicle_number as f32 * car_ratio) as u16;
        let buses = if hour >= 6 && hour <= 9 || hour >= 15 && hour <= 19 {
            self.rng.gen_range(3..10) // More buses during commute hours
        } else {
            self.rng.gen_range(0..5)
        };
        let trucks = if hour >= 9 && hour <= 17 {
            self.rng.gen_range(5..15) // More trucks during business hours
        } else {
            self.rng.gen_range(0..8)
        };
        let motorcycles = (vehicle_number as f32 * 0.05) as u16 + self.rng.gen_range(0..10);

        // Speed correlates with density
        let speed = match density {
            0..=30 => self.rng.gen_range(50..80), // Low traffic - higher speeds
            31..=70 => self.rng.gen_range(30..60), // Medium traffic
            _ => self.rng.gen_range(5..40),       // Heavy traffic - lower speeds
        };

        // Congestion level correlates directly with density
        let congestion_level = match density {
            0..=30 => "low",
            31..=70 => "medium",
            _ => "high",
        };

        let traffic_data = TrafficData {
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            location_id: self.location_id.clone(),
            location_x: self.location_x,
            location_y: self.location_y,
            density,
            travel_time,
            vehicle_number,
            speed,
            direction_change: self.random_choice(&["left", "right", "none"]).to_string(),
            pedestrian_count: self.rng.gen_range(0..50),
            bicycle_count: self.rng.gen_range(0..20),
            heavy_vehicle_count: trucks,
            incident_detected: self.rng.gen_bool(0.1), // 10% chance of incident
            visibility: coordinated_weather.visibility.clone(),
            weather_conditions: coordinated_weather.conditions.clone(),
            road_condition: road_condition.clone(),
            congestion_level: congestion_level.to_string(),
            average_vehicle_size: self
                .random_choice(&["small", "medium", "large"])
                .to_string(),
            vehicle_type_distribution: VehicleTypeDistribution {
                cars,
                buses,
                motorcycles,
                trucks,
            },
            traffic_flow_direction: self
                .random_choice(&["north-south", "east-west", "both"])
                .to_string(),
            red_light_violations: self.rng.gen_range(0..5),
            temperature: coordinated_weather.temperature,
            humidity: coordinated_weather.humidity,
            wind_speed: coordinated_weather.wind_speed,
            air_quality_index: self.rng.gen_range(0..500),
            near_miss_events: self.rng.gen_range(0..5),
            accident_severity: self.random_choice(&["none", "minor", "major"]).to_string(),
            roadwork_detected: self.rng.gen_bool(0.1),
            illegal_parking_cases: self.rng.gen_range(0..10),
            intersection_id: self.intersection_id.clone(),
            sensor_direction: self.sensor_direction.clone(),
            coordinated_weather,
            traffic_light_phase,
            vehicle_flow_rate,
            queue_propagation_factor,
        };

        // Send to Kafka
        let payload = serde_json::to_string(&traffic_data)?;
        self.producer
            .send(
                FutureRecord::to("traffic-data")
                    .payload(&payload)
                    .key(&traffic_data.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        Ok(traffic_data)
    }

    // Add enhanced method for intersection data generation with controller
    async fn generate_intersection_data_with_controller(
        &mut self,
        controller: Arc<Mutex<IntersectionController>>,
    ) -> Result<IntersectionData, Box<dyn std::error::Error>> {
        let hour = Utc::now().hour();

        // Time-based intersection modeling
        let is_rush_hour = hour >= 7 && hour <= 9 || hour >= 16 && hour <= 19;
        let is_business_hours = hour >= 9 && hour <= 17;

        // Generate realistic queue sizes based on time of day
        let queue_base = if is_rush_hour {
            self.rng.gen_range(5..20)
        } else if is_business_hours {
            self.rng.gen_range(2..15)
        } else {
            self.rng.gen_range(0..10)
        };

        let lane1_queue = queue_base + self.rng.gen_range(0..5);
        let lane2_queue = queue_base + self.rng.gen_range(0..5);
        let lane3_queue = queue_base + self.rng.gen_range(0..5);

        // Generate wait times correlated with queue length
        let avg_wait_time = match (lane1_queue + lane2_queue + lane3_queue) / 3 {
            0..=5 => self.rng.gen_range(5..30),
            6..=15 => self.rng.gen_range(30..60),
            _ => self.rng.gen_range(60..120),
        };

        // Get coordinated data from intersection controller
        let controller_guard = controller.lock().await;
        let coordinated_light_status = controller_guard.get_coordinated_light_status();
        let phase_time_remaining = controller_guard.get_phase_time_remaining();
        let intersection_efficiency = controller_guard.calculate_intersection_efficiency();
        let total_intersection_vehicles = controller_guard.get_total_intersection_vehicles();
        let traffic_light_status = controller_guard.get_light_status_for_sensor(&self.sensor_direction);
        let local_weather = controller_guard.get_weather_for_sensor(&self.sensor_id);
        drop(controller_guard);

        // Generate intersection congestion correlated with queues
        let intersection_congestion_level = match (lane1_queue + lane2_queue + lane3_queue) / 3 {
            0..=5 => "low",
            6..=15 => "medium",
            _ => "high",
        };

        // Generate speeds by direction
        let ns_speed = self.rng.gen_range(20..60);
        let ew_speed = self.rng.gen_range(20..60);

        // Pedestrian activity correlates with time of day
        let pedestrians_crossing = if is_business_hours {
            self.rng.gen_range(5..40)
        } else {
            self.rng.gen_range(0..20)
        };

        // Generate intersection data
        let intersection_data = IntersectionData {
            sensor_id: self.sensor_id.clone(),
            timestamp: Utc::now(),
            intersection_id: self.intersection_id.clone(),
            stopped_vehicles_count: lane1_queue + lane2_queue + lane3_queue,
            average_wait_time: avg_wait_time,
            left_turn_count: self.rng.gen_range(0..30),
            right_turn_count: self.rng.gen_range(0..30),
            average_speed_by_direction: AverageSpeedByDirection {
                north_south: ns_speed,
                east_west: ew_speed,
            },
            lane_occupancy: self.rng.gen_range(0..100),
            intersection_blocking_vehicles: if intersection_congestion_level == "high" {
                self.rng.gen_range(0..5)
            } else {
                self.rng.gen_range(0..2)
            },
            traffic_light_compliance_rate: self.rng.gen_range(70..100),
            pedestrians_crossing,
            jaywalking_pedestrians: (pedestrians_crossing as f32 * 0.2) as u16,
            cyclists_crossing: self.rng.gen_range(0..15),
            risky_behavior_detected: self.rng.gen_bool(0.2),
            queue_length_by_lane: QueueLengthByLane {
                lane1: lane1_queue,
                lane2: lane2_queue,
                lane3: lane3_queue,
            },
            intersection_congestion_level: intersection_congestion_level.to_string(),
            intersection_crossing_time: if intersection_congestion_level == "high" {
                self.rng.gen_range(60..120)
            } else {
                self.rng.gen_range(10..60)
            },
            traffic_light_impact: self.random_choice(&["low", "moderate", "high"]).to_string(),
            near_miss_incidents: self.rng.gen_range(0..5),
            collision_count: if self.rng.gen_bool(0.05) {
                self.rng.gen_range(1..3)
            } else {
                0
            },
            sudden_braking_events: self.rng.gen_range(0..10),
            illegal_parking_detected: self.rng.gen_bool(0.2),
            wrong_way_vehicles: if self.rng.gen_bool(0.05) { 1 } else { 0 },
            ambient_light_level: match hour {
                6..=8 => self.rng.gen_range(50..150),   // Dawn
                9..=17 => self.rng.gen_range(150..200), // Day
                18..=20 => self.rng.gen_range(50..150), // Dusk
                _ => self.rng.gen_range(0..50),         // Night
            },
            traffic_light_status,
            local_weather_conditions: local_weather.conditions,
            fog_or_smoke_detected: self.rng.gen_bool(0.15),
            coordinated_light_status,
            phase_time_remaining,
            intersection_efficiency,
            total_intersection_vehicles,
        };

        // Send to Kafka
        let payload = serde_json::to_string(&intersection_data)?;
        self.producer
            .send(
                FutureRecord::to("intersection-data")
                    .payload(&payload)
                    .key(&intersection_data.sensor_id),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(e, _)| Box::<dyn std::error::Error>::from(e))?;

        Ok(intersection_data)
    }
}

// ===== Main Application =====
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create multiple intersection controllers for different areas
    let bd_anfa_controller = Arc::new(Mutex::new(IntersectionController::new(
        "bd-anfa-bd-zerktouni".to_string(),
        vec!["sensor-001".to_string(), "sensor-002".to_string(), "sensor-003".to_string(), "sensor-004".to_string()]
    )));
    
    let hassan_ii_controller = Arc::new(Mutex::new(IntersectionController::new(
        "hassan-ii-bd-moulay-youssef".to_string(),
        vec!["sensor-005".to_string(), "sensor-006".to_string(), "sensor-007".to_string(), "sensor-008".to_string()]
    )));

    // Expanded sensor configs with 8 sensors across 2 intersections
    let sensor_configs = vec![
        // Existing BD Anfa - BD Zerktouni Intersection
        ("sensor-001", "bd-zerktouni-n", 33.5912, -7.6361, "bd-anfa-bd-zerktouni", "north", 750, bd_anfa_controller.clone()),
        ("sensor-002", "bd-zerktouni-s", 33.5907, -7.6357, "bd-anfa-bd-zerktouni", "south", 750, bd_anfa_controller.clone()),  
        ("sensor-003", "bd-anfa-e", 33.5912, -7.6356, "bd-anfa-bd-zerktouni", "east", 750, bd_anfa_controller.clone()),
        ("sensor-004", "bd-anfa-w", 33.5909, -7.6363, "bd-anfa-bd-zerktouni", "west", 750, bd_anfa_controller.clone()),
        
        // NEW Hassan II - Boulevard Moulay Youssef Intersection (2km northeast)
        ("sensor-005", "hassan-ii-n", 33.6045, -7.6142, "hassan-ii-bd-moulay-youssef", "north", 850, hassan_ii_controller.clone()),
        ("sensor-006", "hassan-ii-s", 33.6038, -7.6139, "hassan-ii-bd-moulay-youssef", "south", 850, hassan_ii_controller.clone()),
        ("sensor-007", "bd-moulay-youssef-e", 33.6042, -7.6135, "hassan-ii-bd-moulay-youssef", "east", 850, hassan_ii_controller.clone()),
        ("sensor-008", "bd-moulay-youssef-w", 33.6040, -7.6146, "hassan-ii-bd-moulay-youssef", "west", 850, hassan_ii_controller.clone()),
    ];

    let kafka_brokers = "localhost:9092";
    let periodic_update_interval_s = 60;

    println!("🚀 Starting Enhanced Traffic Sensor Simulator with Multiple Intersection Controllers");
    println!("📡 Kafka broker: {}", kafka_brokers);
    println!("🎯 Sensor count: {}", sensor_configs.len());
    println!("🌦️  Realistic weather: 15-30 minute gradual transitions");
    println!("🚦 Intersections:");
    println!("   • BD Anfa ↔ BD Zerktouni (sensors 001-004)");
    println!("   • Hassan II ↔ BD Moulay Youssef (sensors 005-008)");

    let mut tasks = vec![];

    // Create intersection controller update tasks for each intersection
    let bd_anfa_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1)); // Update every second

        loop {
            interval.tick().await;

            let mut controller = bd_anfa_controller.lock().await;
            controller.update_traffic_lights();
            controller.update_shared_weather(); // Now realistic gradual changes
        }
    });
    tasks.push(bd_anfa_task);

    let hassan_ii_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1)); // Update every second

        loop {
            interval.tick().await;

            let mut controller = hassan_ii_controller.lock().await;
            controller.update_traffic_lights();
            controller.update_shared_weather(); // Independent weather for different area
        }
    });
    tasks.push(hassan_ii_task);

    for (sensor_id, location_id, location_x, location_y, intersection_id, sensor_direction, interval_ms, controller_ref) in
        sensor_configs
    {
        let simulator = Arc::new(Mutex::new(TrafficSimulator::new(
            sensor_id,
            location_id,
            location_x,
            location_y,
            intersection_id,
            sensor_direction,
            kafka_brokers,
        )?));

        // Task for vehicle data generation
        let sim_clone = simulator.clone();
        let vehicle_interval_ms = interval_ms;
        let vehicle_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(vehicle_interval_ms));

            loop {
                interval.tick().await;

                let mut sim = sim_clone.lock().await;
                match sim.generate_vehicle_data().await {
                    Ok(_) => {}
                    Err(e) => eprintln!("❌ Error generating vehicle data for {}: {}", sim.sensor_id, e),
                }
            }
        });
        tasks.push(vehicle_task);

        // Task for aggregated traffic data updates (enhanced with intersection controller)
        let sim_clone = simulator.clone();
        let controller_clone = controller_ref.clone();
        let traffic_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                let mut sim = sim_clone.lock().await;
                let mut controller = controller_clone.lock().await;
                
                // Update controller with current sensor data before generating traffic data
                let sensor_id = sim.sensor_id.clone();
                let density = 50; // This would normally be calculated from current data
                let vehicle_count = 10; // This would normally be from actual vehicle data
                controller.update_vehicle_flow(&sensor_id, vehicle_count, density);
                
                drop(controller); // Release the controller lock
                
                match sim.generate_traffic_data_with_controller(controller_clone.clone()).await {
                    Ok(_) => {}
                    Err(e) => eprintln!("❌ Error generating traffic data for {}: {}", sim.sensor_id, e),
                }
            }
        });
        tasks.push(traffic_task);

        // Task for intersection data updates (enhanced with intersection controller)
        let sim_clone = simulator.clone();
        let controller_clone = controller_ref.clone();
        let intersection_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(3));

            loop {
                interval.tick().await;

                let mut sim = sim_clone.lock().await;
                match sim.generate_intersection_data_with_controller(controller_clone.clone()).await {
                    Ok(_) => {}
                    Err(e) => eprintln!("❌ Error generating intersection data for {}: {}", sim.sensor_id, e),
                }
            }
        });
        tasks.push(intersection_task);

        // Task for health updates (unchanged)
        let sim_clone = simulator.clone();
        let sensor_id_str = sensor_id.to_string();
        let health_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(periodic_update_interval_s));

            loop {
                interval.tick().await;

                println!("📊 Sending health data for sensor {}", sensor_id_str);

                let mut sim = sim_clone.lock().await;
                match sim.send_health_data().await {
                    Ok(_) => {}
                    Err(e) => eprintln!("❌ Error sending health data for {}: {}", sensor_id_str, e),
                }
            }
        });
        tasks.push(health_task);

        println!(
            "✅ Started simulator for sensor {} ({}) at {} with {}ms interval",
            sensor_id, sensor_direction, intersection_id, interval_ms
        );
    }

    // Wait for all tasks to complete (this will run indefinitely)
    futures::future::join_all(tasks).await;

    Ok(())
}
