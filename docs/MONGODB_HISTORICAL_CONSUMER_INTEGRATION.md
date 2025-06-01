# MongoDB Historical Consumer Integration Guide

## Executive Summary

This document provides complete MongoDB integration specifications for long-term historical data storage and analytics from the enhanced Rust Traffic Simulator. The MongoDB consumer complements the real-time PostgreSQL integration by focusing on **time-series data storage**, **historical pattern analysis**, and **long-term trend analytics** with optimized aggregation pipelines.

## 🎯 **MongoDB Integration Architecture**

### Architecture Overview
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Kafka Topics   │───▶│   MongoDB       │───▶│   Analytics     │
│                 │    │   Historical    │    │   Dashboard     │
│  (Enhanced)     │    │   Consumer      │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Features
- **Time-Series Optimization**: Efficient storage for traffic data patterns
- **Aggregation Pipelines**: Pre-computed analytics for dashboard performance
- **Historical Pattern Analysis**: Long-term trend identification
- **Data Archival**: Intelligent data lifecycle management
- **Enhanced Schema**: Optimized for intersection controller coordination data

## 📊 **MongoDB Schema Design**

### Database Structure
```javascript
// Database: traffic_historical
// Collections:
// - traffic_timeseries (main time-series data)
// - intersection_summaries (hourly aggregations)
// - daily_analytics (daily patterns)
// - weekly_patterns (weekly trends)
// - monthly_reports (monthly statistics)
// - sensor_health_history (sensor performance tracking)
// - alert_history (historical alerts and incidents)
// - metadata (schema versions and configurations)
```

### 1. **Traffic Time-Series Collection**

**Collection**: `traffic_timeseries`
**Purpose**: Main time-series storage for all traffic data
**Retention**: 2 years with automated archival

```javascript
// traffic_timeseries schema
{
  _id: ObjectId,
  
  // Time-series partitioning
  timestamp: ISODate("2024-01-15T14:30:22.123Z"),
  date_partition: "2024-01-15", // For efficient querying
  hour_partition: 14,           // For hourly aggregations
  
  // Sensor identification
  sensor_id: "sensor-001",
  intersection_id: "bd-anfa-bd-zerktouni",
  sensor_direction: "north",
  location: {
    coordinates: [-7.6361, 33.5912],
    name: "bd-zerktouni-n"
  },
  
  // Enhanced traffic metrics
  traffic: {
    density: 75,
    vehicle_count: 28,
    avg_speed: 35,
    flow_rate: 72.5,
    congestion_level: "high",
    queue_propagation: 0.65
  },
  
  // Vehicle composition
  vehicles: {
    cars: 22,
    trucks: 3,
    buses: 1,
    motorcycles: 2,
    total: 28
  },
  
  // Coordinated environmental data
  environment: {
    weather: "rain",
    temperature: 18.5,
    humidity: 85,
    visibility: "fair",
    road_condition: "wet"
  },
  
  // Traffic light coordination
  traffic_control: {
    light_phase: "green",
    coordinated_status: "north_south_green",
    phase_remaining: 45,
    cycle_efficiency: 0.78
  },
  
  // Data quality metrics
  quality: {
    confidence: 0.95,
    sensor_health: "healthy",
    message_sequence: 12847
  }
}
```

### 2. **Intersection Summaries Collection**

**Collection**: `intersection_summaries`
**Purpose**: Hourly aggregated intersection-wide metrics
**Retention**: 5 years

```javascript
// intersection_summaries schema
{
  _id: ObjectId,
  
  // Time identification
  timestamp: ISODate("2024-01-15T14:00:00.000Z"),
  date: "2024-01-15",
  hour: 14,
  
  // Intersection identification
  intersection_id: "bd-anfa-bd-zerktouni",
  
  // Aggregated metrics across all 4 sensors
  summary: {
    total_vehicles: 450,
    avg_speed: 32.5,
    peak_density: 85,
    avg_efficiency: 0.76,
    congestion_minutes: 35,
    weather_conditions: ["rain"],
    dominant_weather: "rain"
  },
  
  // Sensor-specific aggregations
  sensors: {
    "sensor-001": {
      direction: "north",
      vehicle_count: 125,
      avg_speed: 34,
      max_density: 80,
      efficiency: 0.78
    },
    "sensor-002": {
      direction: "south", 
      vehicle_count: 118,
      avg_speed: 31,
      max_density: 85,
      efficiency: 0.74
    },
    "sensor-003": {
      direction: "east",
      vehicle_count: 102,
      avg_speed: 33,
      max_density: 70,
      efficiency: 0.79
    },
    "sensor-004": {
      direction: "west",
      vehicle_count: 105,
      avg_speed: 32,
      max_density: 72,
      efficiency: 0.75
    }
  },
  
  // Traffic light performance
  traffic_lights: {
    cycle_count: 40,
    avg_cycle_efficiency: 0.76,
    coordination_uptime: 0.98,
    phase_violations: 2
  },
  
  // Incident summary
  incidents: {
    total_alerts: 3,
    alert_types: ["queue_detected", "wrong_way_driver"],
    avg_resolution_time: 4.5,
    impact_minutes: 12
  }
}
```

### 3. **Daily Analytics Collection**

**Collection**: `daily_analytics`
**Purpose**: Daily pattern analysis and reporting
**Retention**: 7 years

```javascript
// daily_analytics schema  
{
  _id: ObjectId,
  
  // Date identification
  date: "2024-01-15",
  day_of_week: "Monday",
  week_of_year: 3,
  
  // Intersection identification
  intersection_id: "bd-anfa-bd-zerktouni",
  
  // Daily totals
  totals: {
    vehicles: 8750,
    avg_daily_speed: 31.2,
    peak_hour_density: 92,
    congestion_hours: 4.5,
    incidents: 12,
    weather_changes: 2
  },
  
  // Peak analysis
  peaks: {
    morning_rush: {
      start_time: "07:30",
      end_time: "09:15", 
      peak_density: 92,
      avg_speed: 25,
      vehicle_count: 1250
    },
    evening_rush: {
      start_time: "17:00",
      end_time: "19:30",
      peak_density: 88,
      avg_speed: 27,
      vehicle_count: 1380
    }
  },
  
  // Hourly breakdown
  hourly_patterns: [
    {
      hour: 0,
      vehicles: 85,
      avg_speed: 45,
      density: 15,
      incidents: 0
    },
    // ... 24 hours of data
  ],
  
  // Weather impact analysis
  weather_impact: {
    conditions: ["sunny", "rain"],
    rain_hours: 6,
    speed_reduction: 12.5,
    incident_increase: 2.3
  },
  
  // Efficiency metrics
  efficiency: {
    intersection_score: 0.74,
    coordination_uptime: 0.96,
    sensor_availability: 0.99,
    data_quality: 0.97
  }
}
```

## 🔧 **MongoDB Consumer Implementation**

### NPM Dependencies

```json
{
  "dependencies": {
    "kafkajs": "^2.2.4",
    "mongodb": "^6.3.0",
    "moment": "^2.29.4",
    "lodash": "^4.17.21",
    "winston": "^3.10.0",
    "node-cron": "^3.0.3",
    "redis": "^4.6.7"
  }
}
```

### Main Consumer Implementation

```javascript
// mongodb-historical-consumer.js
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');
const winston = require('winston');
const cron = require('node-cron');
const _ = require('lodash');

// Configure logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'mongodb-consumer.log' }),
    new winston.transports.Console()
  ]
});

class MongoDBHistoricalConsumer {
  constructor(config) {
    this.kafka = Kafka({
      clientId: 'mongodb-historical-consumer',
      brokers: config.kafkaBrokers || ['localhost:9092']
    });
    
    this.consumer = this.kafka.consumer({ 
      groupId: 'mongodb-historical-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });
    
    this.mongoUrl = config.mongoUrl || 'mongodb://localhost:27017';
    this.dbName = config.dbName || 'traffic_historical';
    
    this.batchBuffer = new Map(); // For batching writes
    this.batchSize = config.batchSize || 100;
    this.flushInterval = config.flushInterval || 5000; // 5 seconds
  }

  async start() {
    try {
      // Connect to MongoDB
      this.mongoClient = new MongoClient(this.mongoUrl);
      await this.mongoClient.connect();
      this.db = this.mongoClient.db(this.dbName);
      
      logger.info('Connected to MongoDB');
      
      // Create indexes for optimal performance
      await this.createIndexes();
      
      // Connect to Kafka
      await this.consumer.connect();
      
      // Subscribe to topics
      await this.consumer.subscribe({
        topics: [
          'traffic-data',
          'intersection-data', 
          'sensor-health',
          'traffic-alerts'
        ],
        fromBeginning: false
      });

      // Start consuming
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            await this.processMessage(topic, data);
          } catch (error) {
            logger.error(`Error processing message from ${topic}:`, error);
          }
        }
      });

      // Start batch flush timer
      setInterval(() => this.flushBatches(), this.flushInterval);
      
      // Schedule aggregation jobs
      this.scheduleAggregationJobs();
      
      logger.info('MongoDB Historical Consumer started successfully');
      
    } catch (error) {
      logger.error('Failed to start MongoDB consumer:', error);
      throw error;
    }
  }

  async createIndexes() {
    const collections = {
      traffic_timeseries: [
        { timestamp: -1 },
        { sensor_id: 1, timestamp: -1 },
        { intersection_id: 1, timestamp: -1 },
        { date_partition: 1, hour_partition: 1 },
        { "location.coordinates": "2dsphere" }
      ],
      intersection_summaries: [
        { intersection_id: 1, timestamp: -1 },
        { date: 1, hour: 1 }
      ],
      daily_analytics: [
        { intersection_id: 1, date: -1 },
        { date: -1 }
      ],
      sensor_health_history: [
        { sensor_id: 1, timestamp: -1 }
      ],
      alert_history: [
        { intersection_id: 1, timestamp: -1 },
        { "alert_type": 1, timestamp: -1 }
      ]
    };

    for (const [collectionName, indexes] of Object.entries(collections)) {
      const collection = this.db.collection(collectionName);
      for (const index of indexes) {
        await collection.createIndex(index);
      }
      logger.info(`Created indexes for ${collectionName}`);
    }
  }

  async processMessage(topic, data) {
    const batchKey = `${topic}-${Math.floor(Date.now() / this.flushInterval)}`;
    
    if (!this.batchBuffer.has(batchKey)) {
      this.batchBuffer.set(batchKey, []);
    }
    
    const transformedData = await this.transformData(topic, data);
    this.batchBuffer.get(batchKey).push(transformedData);
    
    // Flush if batch is full
    if (this.batchBuffer.get(batchKey).length >= this.batchSize) {
      await this.flushBatch(batchKey);
    }
  }

  async transformData(topic, data) {
    const timestamp = new Date(data.timestamp);
    const datePartition = timestamp.toISOString().split('T')[0];
    const hourPartition = timestamp.getHours();
    
    switch (topic) {
      case 'traffic-data':
        return {
          timestamp,
          date_partition: datePartition,
          hour_partition: hourPartition,
          sensor_id: data.sensor_id,
          intersection_id: data.intersection_id,
          sensor_direction: data.sensor_direction,
          location: {
            coordinates: [data.location_x, data.location_y],
            name: data.location_id
          },
          traffic: {
            density: data.density,
            vehicle_count: data.vehicle_number,
            avg_speed: data.speed,
            flow_rate: data.vehicle_flow_rate,
            congestion_level: data.congestion_level,
            queue_propagation: data.queue_propagation_factor
          },
          vehicles: {
            cars: data.vehicle_type_distribution?.cars || 0,
            trucks: data.vehicle_type_distribution?.trucks || 0,
            buses: data.vehicle_type_distribution?.buses || 0,
            motorcycles: data.vehicle_type_distribution?.motorcycles || 0,
            total: data.vehicle_number
          },
          environment: {
            weather: data.coordinated_weather?.conditions || data.weather_conditions,
            temperature: data.coordinated_weather?.temperature || data.temperature,
            humidity: data.coordinated_weather?.humidity || data.humidity,
            visibility: data.coordinated_weather?.visibility || data.visibility,
            road_condition: data.coordinated_weather?.road_condition || data.road_condition
          },
          traffic_control: {
            light_phase: data.traffic_light_phase,
            coordinated_status: data.coordinated_light_status,
            phase_remaining: data.phase_time_remaining,
            cycle_efficiency: data.intersection_efficiency
          },
          quality: {
            confidence: 0.95, // Computed based on data validation
            sensor_health: "healthy", // From sensor health data
            message_sequence: data.counter || 0
          }
        };
        
      case 'intersection-data':
        return {
          timestamp,
          date_partition: datePartition,
          hour_partition: hourPartition,
          intersection_id: data.intersection_id,
          sensor_id: data.sensor_id,
          coordination: {
            light_status: data.coordinated_light_status,
            phase_remaining: data.phase_time_remaining,
            efficiency: data.intersection_efficiency,
            total_vehicles: data.total_intersection_vehicles
          },
          traffic_metrics: {
            stopped_vehicles: data.stopped_vehicles_count,
            avg_wait_time: data.average_wait_time,
            congestion_level: data.intersection_congestion_level,
            crossing_time: data.intersection_crossing_time
          }
        };
        
      case 'sensor-health':
        return {
          timestamp,
          date_partition: datePartition,
          sensor_id: data.sensor_id,
          health_metrics: {
            battery_level: data.battery_level,
            temperature: data.temperature_c,
            hw_fault: data.hw_fault,
            low_voltage: data.low_voltage,
            uptime: data.uptime_s,
            message_count: data.message_count
          }
        };
        
      case 'traffic-alerts':
        return {
          timestamp,
          date_partition: datePartition,
          alert_type: data.type,
          sensor_id: data.sensor_id,
          intersection_id: data.vehicle_data?.intersection_id,
          alert_details: {
            vehicle_data: data.vehicle_data,
            severity: this.calculateAlertSeverity(data.type),
            impact_estimate: this.estimateImpact(data)
          }
        };
        
      default:
        return data;
    }
  }

  async flushBatches() {
    const promises = [];
    for (const batchKey of this.batchBuffer.keys()) {
      promises.push(this.flushBatch(batchKey));
    }
    await Promise.all(promises);
  }

  async flushBatch(batchKey) {
    const batch = this.batchBuffer.get(batchKey);
    if (!batch || batch.length === 0) return;
    
    try {
      const topic = batchKey.split('-')[0];
      const collectionName = this.getCollectionName(topic);
      
      await this.db.collection(collectionName).insertMany(batch, { ordered: false });
      
      logger.info(`Flushed ${batch.length} documents to ${collectionName}`);
      this.batchBuffer.delete(batchKey);
      
    } catch (error) {
      logger.error(`Error flushing batch ${batchKey}:`, error);
    }
  }

  getCollectionName(topic) {
    const mapping = {
      'traffic-data': 'traffic_timeseries',
      'intersection-data': 'intersection_timeseries', 
      'sensor-health': 'sensor_health_history',
      'traffic-alerts': 'alert_history'
    };
    return mapping[topic] || 'unknown_data';
  }

  scheduleAggregationJobs() {
    // Hourly aggregation (runs at 5 minutes past each hour)
    cron.schedule('5 * * * *', async () => {
      await this.runHourlyAggregation();
    });
    
    // Daily aggregation (runs at 1:05 AM)
    cron.schedule('5 1 * * *', async () => {
      await this.runDailyAggregation();
    });
    
    // Weekly aggregation (runs Sunday at 2:05 AM)
    cron.schedule('5 2 * * 0', async () => {
      await this.runWeeklyAggregation();
    });
    
    logger.info('Scheduled aggregation jobs');
  }

  async runHourlyAggregation() {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    const hourStart = new Date(oneHourAgo);
    hourStart.setMinutes(0, 0, 0);
    
    const hourEnd = new Date(hourStart);
    hourEnd.setHours(hourEnd.getHours() + 1);
    
    logger.info(`Running hourly aggregation for ${hourStart.toISOString()}`);
    
    try {
      const pipeline = [
        {
          $match: {
            timestamp: { $gte: hourStart, $lt: hourEnd },
            intersection_id: { $exists: true }
          }
        },
        {
          $group: {
            _id: {
              intersection_id: "$intersection_id",
              hour: { $hour: "$timestamp" },
              date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } }
            },
            total_vehicles: { $sum: "$traffic.vehicle_count" },
            avg_speed: { $avg: "$traffic.avg_speed" },
            peak_density: { $max: "$traffic.density" },
            avg_efficiency: { $avg: "$traffic_control.cycle_efficiency" },
            weather_conditions: { $addToSet: "$environment.weather" },
            sensor_data: {
              $push: {
                sensor_id: "$sensor_id",
                direction: "$sensor_direction",
                vehicle_count: "$traffic.vehicle_count",
                avg_speed: "$traffic.avg_speed",
                density: "$traffic.density",
                efficiency: "$traffic_control.cycle_efficiency"
              }
            }
          }
        },
        {
          $project: {
            timestamp: {
              $dateFromParts: {
                year: { $toInt: { $substr: ["$_id.date", 0, 4] } },
                month: { $toInt: { $substr: ["$_id.date", 5, 2] } },
                day: { $toInt: { $substr: ["$_id.date", 8, 2] } },
                hour: "$_id.hour"
              }
            },
            date: "$_id.date",
            hour: "$_id.hour",
            intersection_id: "$_id.intersection_id",
            summary: {
              total_vehicles: "$total_vehicles",
              avg_speed: "$avg_speed",
              peak_density: "$peak_density",
              avg_efficiency: "$avg_efficiency",
              weather_conditions: "$weather_conditions",
              dominant_weather: { $arrayElemAt: ["$weather_conditions", 0] }
            },
            sensors: {
              $arrayToObject: {
                $map: {
                  input: {
                    $setUnion: [
                      { $map: { input: "$sensor_data", as: "s", in: "$$s.sensor_id" } }
                    ]
                  },
                  as: "sensor_id",
                  in: {
                    k: "$$sensor_id",
                    v: {
                      $let: {
                        vars: {
                          sensorData: {
                            $filter: {
                              input: "$sensor_data",
                              cond: { $eq: ["$$this.sensor_id", "$$sensor_id"] }
                            }
                          }
                        },
                        in: {
                          direction: { $arrayElemAt: ["$$sensorData.direction", 0] },
                          vehicle_count: { $sum: "$$sensorData.vehicle_count" },
                          avg_speed: { $avg: "$$sensorData.avg_speed" },
                          max_density: { $max: "$$sensorData.density" },
                          efficiency: { $avg: "$$sensorData.efficiency" }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      ];
      
      const results = await this.db.collection('traffic_timeseries').aggregate(pipeline).toArray();
      
      if (results.length > 0) {
        await this.db.collection('intersection_summaries').insertMany(results, { ordered: false });
        logger.info(`Created ${results.length} hourly summary documents`);
      }
      
    } catch (error) {
      logger.error('Error in hourly aggregation:', error);
    }
  }

  async runDailyAggregation() {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const dateStr = yesterday.toISOString().split('T')[0];
    
    logger.info(`Running daily aggregation for ${dateStr}`);
    
    try {
      const pipeline = [
        {
          $match: {
            date: dateStr
          }
        },
        {
          $group: {
            _id: {
              intersection_id: "$intersection_id",
              date: "$date"
            },
            totals: {
              $first: {
                vehicles: { $sum: "$summary.total_vehicles" },
                avg_daily_speed: { $avg: "$summary.avg_speed" },
                peak_hour_density: { $max: "$summary.peak_density" },
                weather_changes: { $size: { $setUnion: ["$summary.weather_conditions"] } }
              }
            },
            hourly_patterns: {
              $push: {
                hour: "$hour",
                vehicles: "$summary.total_vehicles",
                avg_speed: "$summary.avg_speed",
                density: "$summary.peak_density"
              }
            },
            efficiency: {
              $first: {
                intersection_score: { $avg: "$summary.avg_efficiency" }
              }
            }
          }
        },
        {
          $project: {
            date: "$_id.date",
            day_of_week: {
              $dayOfWeek: {
                $dateFromString: { dateString: "$_id.date" }
              }
            },
            week_of_year: {
              $week: {
                $dateFromString: { dateString: "$_id.date" }
              }
            },
            intersection_id: "$_id.intersection_id",
            totals: "$totals",
            hourly_patterns: "$hourly_patterns",
            efficiency: "$efficiency"
          }
        }
      ];
      
      const results = await this.db.collection('intersection_summaries').aggregate(pipeline).toArray();
      
      if (results.length > 0) {
        await this.db.collection('daily_analytics').insertMany(results, { ordered: false });
        logger.info(`Created ${results.length} daily analytics documents`);
      }
      
    } catch (error) {
      logger.error('Error in daily aggregation:', error);
    }
  }
}

// Start the consumer
const consumer = new MongoDBHistoricalConsumer({
  kafkaBrokers: ['localhost:9092'],
  mongoUrl: 'mongodb://localhost:27017',
  dbName: 'traffic_historical',
  batchSize: 100,
  flushInterval: 5000
});

consumer.start().catch(console.error);

module.exports = MongoDBHistoricalConsumer;
```

## 📈 **Analytics & Aggregation Pipelines**

### 1. **Traffic Pattern Analysis**

```javascript
// Weekly pattern analysis
async function getWeeklyTrafficPatterns(intersectionId, startDate, endDate) {
  const pipeline = [
    {
      $match: {
        intersection_id: intersectionId,
        date: { $gte: startDate, $lte: endDate }
      }
    },
    {
      $group: {
        _id: {
          day_of_week: "$day_of_week",
          hour: { $arrayElemAt: ["$hourly_patterns.hour", 0] }
        },
        avg_vehicles: { $avg: { $arrayElemAt: ["$hourly_patterns.vehicles", 0] } },
        avg_speed: { $avg: { $arrayElemAt: ["$hourly_patterns.avg_speed", 0] } },
        pattern_consistency: { $stdDevPop: { $arrayElemAt: ["$hourly_patterns.vehicles", 0] } }
      }
    },
    {
      $group: {
        _id: "$_id.day_of_week",
        hourly_pattern: {
          $push: {
            hour: "$_id.hour",
            avg_vehicles: "$avg_vehicles",
            avg_speed: "$avg_speed",
            consistency: "$pattern_consistency"
          }
        },
        daily_total: { $sum: "$avg_vehicles" }
      }
    },
    {
      $sort: { "_id": 1 }
    }
  ];
  
  return await db.collection('daily_analytics').aggregate(pipeline).toArray();
}
```

### 2. **Intersection Efficiency Analysis**

```javascript
// Intersection efficiency trends
async function getIntersectionEfficiencyTrends(intersectionId, days = 30) {
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  
  const pipeline = [
    {
      $match: {
        intersection_id: intersectionId,
        timestamp: { $gte: startDate }
      }
    },
    {
      $group: {
        _id: {
          date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } },
          hour: { $hour: "$timestamp" }
        },
        avg_efficiency: { $avg: "$summary.avg_efficiency" },
        total_vehicles: { $sum: "$summary.total_vehicles" },
        coordination_quality: {
          $avg: {
            $cond: [
              { $gte: ["$summary.avg_efficiency", 0.7] },
              1,
              0
            ]
          }
        }
      }
    },
    {
      $project: {
        timestamp: {
          $dateFromParts: {
            year: { $toInt: { $substr: ["$_id.date", 0, 4] } },
            month: { $toInt: { $substr: ["$_id.date", 5, 2] } },
            day: { $toInt: { $substr: ["$_id.date", 8, 2] } },
            hour: "$_id.hour"
          }
        },
        efficiency: "$avg_efficiency",
        volume: "$total_vehicles",
        quality_score: "$coordination_quality"
      }
    },
    {
      $sort: { timestamp: 1 }
    }
  ];
  
  return await db.collection('intersection_summaries').aggregate(pipeline).toArray();
}
```

### 3. **Weather Impact Analysis**

```javascript
// Weather impact on traffic flow
async function analyzeWeatherImpact(intersectionId, months = 3) {
  const startDate = new Date();
  startDate.setMonth(startDate.getMonth() - months);
  
  const pipeline = [
    {
      $match: {
        timestamp: { $gte: startDate },
        intersection_id: intersectionId
      }
    },
    {
      $group: {
        _id: "$environment.weather",
        avg_speed: { $avg: "$traffic.avg_speed" },
        avg_density: { $avg: "$traffic.density" },
        avg_flow_rate: { $avg: "$traffic.flow_rate" },
        total_incidents: { $sum: { $cond: [{ $gt: ["$incidents.total_alerts", 0] }, 1, 0] } },
        sample_count: { $sum: 1 }
      }
    },
    {
      $project: {
        weather_condition: "$_id",
        impact_metrics: {
          avg_speed: { $round: ["$avg_speed", 1] },
          avg_density: { $round: ["$avg_density", 1] },
          avg_flow_rate: { $round: ["$avg_flow_rate", 1] },
          incident_rate: { $round: [{ $divide: ["$total_incidents", "$sample_count"] }, 3] }
        },
        data_points: "$sample_count"
      }
    },
    {
      $sort: { "impact_metrics.avg_speed": -1 }
    }
  ];
  
  return await db.collection('traffic_timeseries').aggregate(pipeline).toArray();
}
```

## 🔍 **Query Examples & API Integration**

### REST API Endpoints

```javascript
// express-mongodb-api.js
const express = require('express');
const { MongoClient } = require('mongodb');
const app = express();

app.use(express.json());

// MongoDB connection
let db;
MongoClient.connect('mongodb://localhost:27017/traffic_historical')
  .then(client => {
    db = client.db('traffic_historical');
    console.log('Connected to MongoDB');
  });

// Get historical traffic data for specific time range
app.get('/api/historical/traffic/:intersectionId', async (req, res) => {
  try {
    const { intersectionId } = req.params;
    const { startDate, endDate, granularity = 'hour' } = req.query;
    
    let collection, groupBy;
    
    switch (granularity) {
      case 'minute':
        collection = 'traffic_timeseries';
        groupBy = {
          year: { $year: "$timestamp" },
          month: { $month: "$timestamp" },
          day: { $dayOfMonth: "$timestamp" },
          hour: { $hour: "$timestamp" },
          minute: { $minute: "$timestamp" }
        };
        break;
      case 'hour':
        collection = 'intersection_summaries';
        groupBy = {
          year: { $year: "$timestamp" },
          month: { $month: "$timestamp" },
          day: { $dayOfMonth: "$timestamp" },
          hour: { $hour: "$timestamp" }
        };
        break;
      case 'day':
        collection = 'daily_analytics';
        groupBy = {
          year: { $year: { $dateFromString: { dateString: "$date" } } },
          month: { $month: { $dateFromString: { dateString: "$date" } } },
          day: { $dayOfMonth: { $dateFromString: { dateString: "$date" } } }
        };
        break;
    }
    
    const pipeline = [
      {
        $match: {
          intersection_id: intersectionId,
          ...(startDate && endDate ? {
            $and: [
              collection === 'daily_analytics' 
                ? { date: { $gte: startDate } }
                : { timestamp: { $gte: new Date(startDate) } },
              collection === 'daily_analytics'
                ? { date: { $lte: endDate } }
                : { timestamp: { $lte: new Date(endDate) } }
            ]
          } : {})
        }
      },
      {
        $group: {
          _id: groupBy,
          avg_vehicles: { $avg: collection === 'traffic_timeseries' ? "$traffic.vehicle_count" : "$summary.total_vehicles" },
          avg_speed: { $avg: collection === 'traffic_timeseries' ? "$traffic.avg_speed" : "$summary.avg_speed" },
          avg_density: { $avg: collection === 'traffic_timeseries' ? "$traffic.density" : "$summary.peak_density" },
          efficiency: { $avg: collection === 'traffic_timeseries' ? "$traffic_control.cycle_efficiency" : "$summary.avg_efficiency" }
        }
      },
      {
        $project: {
          timestamp: {
            $dateFromParts: {
              year: "$_id.year",
              month: "$_id.month", 
              day: "$_id.day",
              hour: "$_id.hour",
              minute: "$_id.minute"
            }
          },
          metrics: {
            vehicles: { $round: ["$avg_vehicles", 0] },
            speed: { $round: ["$avg_speed", 1] },
            density: { $round: ["$avg_density", 1] },
            efficiency: { $round: ["$efficiency", 3] }
          }
        }
      },
      {
        $sort: { timestamp: 1 }
      }
    ];
    
    const results = await db.collection(collection).aggregate(pipeline).toArray();
    res.json(results);
    
  } catch (error) {
    console.error('Error fetching historical data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get traffic patterns analysis
app.get('/api/analytics/patterns/:intersectionId', async (req, res) => {
  try {
    const { intersectionId } = req.params;
    const { type = 'weekly' } = req.query;
    
    let pipeline;
    
    switch (type) {
      case 'weekly':
        pipeline = [
          {
            $match: { intersection_id: intersectionId }
          },
          {
            $group: {
              _id: {
                day_of_week: "$day_of_week",
                hour: { $arrayElemAt: ["$hourly_patterns.hour", 0] }
              },
              avg_vehicles: { $avg: { $arrayElemAt: ["$hourly_patterns.vehicles", 0] } },
              consistency: { $stdDevPop: { $arrayElemAt: ["$hourly_patterns.vehicles", 0] } }
            }
          },
          {
            $group: {
              _id: "$_id.day_of_week",
              pattern: {
                $push: {
                  hour: "$_id.hour",
                  vehicles: { $round: ["$avg_vehicles", 0] },
                  consistency: { $round: ["$consistency", 2] }
                }
              }
            }
          },
          {
            $sort: { "_id": 1 }
          }
        ];
        break;
        
      case 'seasonal':
        pipeline = [
          {
            $match: { intersection_id: intersectionId }
          },
          {
            $group: {
              _id: {
                month: { $month: { $dateFromString: { dateString: "$date" } } },
                day_type: {
                  $cond: [
                    { $in: ["$day_of_week", [1, 7]] },
                    "weekend",
                    "weekday"
                  ]
                }
              },
              avg_daily_vehicles: { $avg: "$totals.vehicles" },
              avg_speed: { $avg: "$totals.avg_daily_speed" },
              efficiency: { $avg: "$efficiency.intersection_score" }
            }
          },
          {
            $sort: { "_id.month": 1, "_id.day_type": 1 }
          }
        ];
        break;
    }
    
    const results = await db.collection('daily_analytics').aggregate(pipeline).toArray();
    res.json(results);
    
  } catch (error) {
    console.error('Error fetching pattern analysis:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get efficiency analytics
app.get('/api/analytics/efficiency/:intersectionId', async (req, res) => {
  try {
    const { intersectionId } = req.params;
    const { days = 30 } = req.query;
    
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - parseInt(days));
    
    const pipeline = [
      {
        $match: {
          intersection_id: intersectionId,
          timestamp: { $gte: startDate }
        }
      },
      {
        $group: {
          _id: {
            date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } }
          },
          daily_efficiency: { $avg: "$summary.avg_efficiency" },
          total_vehicles: { $sum: "$summary.total_vehicles" },
          peak_efficiency: { $max: "$summary.avg_efficiency" },
          min_efficiency: { $min: "$summary.avg_efficiency" }
        }
      },
      {
        $project: {
          date: "$_id.date",
          efficiency_metrics: {
            average: { $round: ["$daily_efficiency", 3] },
            peak: { $round: ["$peak_efficiency", 3] },
            minimum: { $round: ["$min_efficiency", 3] },
            consistency: {
              $round: [
                { $subtract: [1, { $divide: [{ $subtract: ["$peak_efficiency", "$min_efficiency"] }, "$daily_efficiency"] }] },
                3
              ]
            }
          },
          volume: "$total_vehicles"
        }
      },
      {
        $sort: { date: 1 }
      }
    ];
    
    const results = await db.collection('intersection_summaries').aggregate(pipeline).toArray();
    
    // Calculate overall statistics
    const overallStats = {
      period_average: results.reduce((sum, day) => sum + day.efficiency_metrics.average, 0) / results.length,
      trend: results.length > 1 ? (results[results.length - 1].efficiency_metrics.average - results[0].efficiency_metrics.average) : 0,
      total_volume: results.reduce((sum, day) => sum + day.volume, 0)
    };
    
    res.json({
      daily_efficiency: results,
      summary: overallStats
    });
    
  } catch (error) {
    console.error('Error fetching efficiency analytics:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(3002, () => {
  console.log('MongoDB Historical API listening on port 3002');
});
```

## 🚀 **Deployment & Configuration**

### Docker Compose for MongoDB Setup

```yaml
# docker-compose-mongodb.yml
version: '3.8'
services:
  mongodb:
    image: mongo:7.0
    container_name: mongodb-traffic
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: traffic_admin_pass
      MONGO_INITDB_DATABASE: traffic_historical
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db
      - ./mongo-init:/docker-entrypoint-initdb.d
    command: --wiredTigerCacheSizeGB 2

  mongodb-consumer:
    build:
      context: .
      dockerfile: Dockerfile.mongodb-consumer
    container_name: mongodb-historical-consumer
    depends_on:
      - mongodb
      - kafka
    environment:
      KAFKA_BROKERS: kafka:29092
      MONGO_URL: mongodb://admin:traffic_admin_pass@mongodb:27017/traffic_historical?authSource=admin
      LOG_LEVEL: info
    volumes:
      - ./logs:/app/logs

volumes:
  mongodb_data:
```

### MongoDB Consumer Dockerfile

```dockerfile
# Dockerfile.mongodb-consumer
FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Expose port for health checks
EXPOSE 3003

# Start the consumer
CMD ["node", "mongodb-historical-consumer.js"]
```

## 📊 **Monitoring & Performance**

### Performance Monitoring Queries

```javascript
// Monitor collection sizes
async function getCollectionStats() {
  const collections = ['traffic_timeseries', 'intersection_summaries', 'daily_analytics'];
  const stats = {};
  
  for (const collection of collections) {
    const collStats = await db.collection(collection).stats();
    stats[collection] = {
      count: collStats.count,
      avgObjSize: Math.round(collStats.avgObjSize),
      storageSize: Math.round(collStats.storageSize / 1024 / 1024), // MB
      indexSize: Math.round(collStats.totalIndexSize / 1024 / 1024), // MB
    };
  }
  
  return stats;
}

// Monitor write performance
async function getWritePerformance() {
  const result = await db.runCommand({ serverStatus: 1 });
  return {
    insertsPerSecond: result.opcounters.insert,
    updatesPerSecond: result.opcounters.update,
    avgWriteTime: result.globalLock.totalTime
  };
}
```

### Health Check Endpoint

```javascript
// Add to express app
app.get('/health', async (req, res) => {
  try {
    // Check MongoDB connection
    await db.admin().ping();
    
    // Check recent data ingestion
    const recentCount = await db.collection('traffic_timeseries').countDocuments({
      timestamp: { $gte: new Date(Date.now() - 5 * 60 * 1000) } // Last 5 minutes
    });
    
    // Check collection stats
    const stats = await getCollectionStats();
    
    res.json({
      status: 'healthy',
      mongodb: 'connected',
      recent_ingestion: recentCount,
      collection_stats: stats,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});
```

## 🎯 **Expected Benefits**

### 📈 **Data Analytics Capabilities**
- **Long-term Trend Analysis**: 2+ years of historical traffic patterns
- **Seasonal Pattern Recognition**: Weather impact and seasonal variations
- **Efficiency Optimization**: Intersection performance trending
- **Predictive Insights**: Pattern-based traffic forecasting

### ⚡ **Performance Optimizations**
- **Batch Processing**: 100-document batches for efficient writes
- **Time-Series Indexing**: Optimized queries for temporal data
- **Pre-computed Aggregations**: Hourly/daily/weekly summaries
- **Intelligent Archival**: Automated data lifecycle management

### 🔍 **Analytics Features**
- **Traffic Volume Patterns**: Peak identification and trend analysis
- **Speed/Congestion Correlation**: Impact analysis across conditions
- **Weather Impact Studies**: Quantified weather effects on traffic
- **Intersection Efficiency**: Coordination effectiveness tracking

This MongoDB integration provides Big Daddy with comprehensive historical analysis capabilities while maintaining optimal performance for both data ingestion and analytics queries! 🚀 