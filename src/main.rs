#![warn(keyword_idents_2024)]
use chrono::{DateTime, Datelike, Timelike, Utc};
use futures::future::join_all;
use rand::{
    distributions::{Distribution, Standard, Uniform},
    rngs::StdRng,
    seq::SliceRandom,
    thread_rng, Rng, SeedableRng,
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
use tokio::{sync::Mutex, task, time};
use uuid::Uuid;

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
}

// ===== Vehicle Simulator Logic =====

impl TrafficSimulator {
    fn new(
        sensor_id: &str,
        location_id: &str,
        location_x: f32,
        location_y: f32,
        intersection_id: &str,
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
            6..=9 => vec![70, 15, 5, 2, 5, 2, 1], // More buses in morning commute
            16..=19 => vec![65, 20, 8, 2, 2, 1, 2], // More cars/SUVs in evening
            9..=17 => vec![50, 20, 10, 5, 2, 8, 5], // More delivery vehicles midday
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
            6..=9 | 16..=19 => 0.7, // Rush hour - more cars
            9..=16 => 0.65,         // Business hours - more trucks/deliveries
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
}

// ===== Main Application =====
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sensor_configs = vec![
        (
            "sensor-001",
            "bd-zerktouni-n",
            33.5912,
            -7.6361,
            "bd-anfa-bd-zerktouni",
            750,
        ),
        (
            "sensor-002",
            "bd-zerktouni-s",
            33.5907,
            -7.6357,
            "bd-anfa-bd-zerktouni",
            750,
        ),
        (
            "sensor-003",
            "bd-anfa-e",
            33.5912,
            -7.6356,
            "bd-anfa-bd-zerktouni",
            750,
        ),
        (
            "sensor-004",
            "bd-anfa-w",
            33.5909,
            -7.6363,
            "bd-anfa-bd-zerktouni",
            750,
        ),
    ];

    let kafka_brokers = "localhost:9092";
    let periodic_update_interval_s = 60;

    println!("Starting Traffic Sensor Simulator");
    println!("Kafka broker: {}", kafka_brokers);
    println!("Sensor count: {}", sensor_configs.len());

    let mut tasks = vec![];

    for (sensor_id, location_id, location_x, location_y, intersection_id, interval_ms) in
        sensor_configs
    {
        let simulator = Arc::new(Mutex::new(TrafficSimulator::new(
            sensor_id,
            location_id,
            location_x,
            location_y,
            intersection_id,
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
                    Err(e) => eprintln!("Error generating vehicle data: {}", e),
                }
            }
        });
        tasks.push(vehicle_task);

        // Task for aggregated traffic data updates
        let sim_clone = simulator.clone();
        let traffic_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                let mut sim = sim_clone.lock().await;
                match sim.generate_traffic_data().await {
                    Ok(_) => {}
                    Err(e) => eprintln!("Error generating traffic data: {}", e),
                }
            }
        });
        tasks.push(traffic_task);

        // Task for intersection data updates
        let sim_clone = simulator.clone();
        let intersection_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(3));

            loop {
                interval.tick().await;

                let mut sim = sim_clone.lock().await;
                match sim.generate_intersection_data().await {
                    Ok(_) => {}
                    Err(e) => eprintln!("Error generating intersection data: {}", e),
                }
            }
        });
        tasks.push(intersection_task);

        // Task for health updates
        let sim_clone = simulator.clone();
        let sensor_id_str = sensor_id.to_string();
        let health_task = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(periodic_update_interval_s));

            loop {
                interval.tick().await;

                println!("Sending health data for sensor {}", sensor_id_str);

                let mut sim = sim_clone.lock().await;
                match sim.send_health_data().await {
                    Ok(_) => {}
                    Err(e) => eprintln!("Error sending health data: {}", e),
                }
            }
        });
        tasks.push(health_task);

        println!(
            "Started simulator for sensor {} with interval {}ms",
            sensor_id, interval_ms
        );
    }

    // Wait for all tasks to complete (this will run indefinitely)
    futures::future::join_all(tasks).await;

    Ok(())
}
