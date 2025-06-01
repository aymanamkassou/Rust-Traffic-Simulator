# Integration Architecture Plan

## Overview

This document outlines the complete integration architecture for the Traffic Data Collection and Reporting IoT system, spanning from the enhanced Rust Traffic Simulator through Kafka broker to Express.js backend and finally to the NextJS dashboard frontend.

## Architecture Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Rust Traffic   │───▶│  Apache Kafka   │───▶│   Express.js    │───▶│   NextJS        │
│   Simulator     │    │    Broker       │    │    Backend      │    │   Dashboard     │
│   (Producer)    │    │                 │    │   (Consumer)    │    │  (Frontend)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Phase 1: Kafka Infrastructure Enhancement

### 1.1 Kafka Topics Architecture

#### Current Topics (Existing)
```yaml
Topics:
  raw-vehicle-data:
    partitions: 4
    replication-factor: 1
    retention: 24h
    
  traffic-data:
    partitions: 4
    replication-factor: 1
    retention: 7d
    
  intersection-data:
    partitions: 4
    replication-factor: 1
    retention: 7d
    
  sensor-health:
    partitions: 2
    replication-factor: 1
    retention: 30d
    
  traffic-alerts:
    partitions: 2
    replication-factor: 1
    retention: 30d
```

#### New Topics (To Add)
```yaml
Enhanced Topics:
  intersection-summary:
    partitions: 4
    replication-factor: 1
    retention: 7d
    description: "Aggregated intersection-wide metrics"
    
  traffic-patterns:
    partitions: 2
    replication-factor: 1
    retention: 30d
    description: "Historical traffic pattern analysis"
    
  system-events:
    partitions: 1
    replication-factor: 1
    retention: 7d
    description: "System status and operational events"
    
  dashboard-metrics:
    partitions: 2
    replication-factor: 1
    retention: 24h
    description: "Pre-processed metrics for dashboard consumption"
```

### 1.2 Enhanced Docker Compose Configuration

**File**: `docker-compose.enhanced.yml`

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_CONFLUENT_SUPPORT_CUSTOMER_ID: anonymous
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092

  redis:
    image: redis:7-alpine
    container_name: redis
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

  postgres:
    image: postgres:15-alpine
    container_name: postgres
    environment:
      POSTGRES_USER: traffic_user
      POSTGRES_PASSWORD: traffic_pass
      POSTGRES_DB: traffic_db
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./init-db:/docker-entrypoint-initdb.d

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
  redis-data:
  postgres-data:
```

### 1.3 Kafka Topic Creation Script

**File**: `scripts/create-kafka-topics.sh`

```bash
#!/bin/bash

KAFKA_CONTAINER="kafka"
TOPICS=(
    "raw-vehicle-data:4:1"
    "traffic-data:4:1"
    "intersection-data:4:1"
    "sensor-health:2:1"
    "traffic-alerts:2:1"
    "intersection-summary:4:1"
    "traffic-patterns:2:1"
    "system-events:1:1"
    "dashboard-metrics:2:1"
)

for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    
    echo "Creating topic: $topic"
    docker exec $KAFKA_CONTAINER kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic $topic \
        --partitions $partitions \
        --replication-factor $replication \
        --if-not-exists
done

echo "Topics created successfully!"
```

## Phase 2: Express.js Backend Implementation

### 2.1 Backend Architecture

```
express-backend/
├── src/
│   ├── config/
│   │   ├── database.js
│   │   ├── kafka.js
│   │   ├── redis.js
│   │   └── kafka.js
│   ├── consumers/
│   │   ├── vehicleConsumer.js
│   │   ├── trafficConsumer.js
│   │   ├── intersectionConsumer.js
│   │   └── alertConsumer.js
│   ├── controllers/
│   │   ├── trafficController.js
│   │   ├── intersectionController.js
│   │   ├── alertController.js
│   │   └── analyticsController.js
│   ├── models/
│   │   ├── Vehicle.js
│   │   ├── Traffic.js
│   │   ├── Intersection.js
│   │   └── Alert.js
│   ├── routes/
│   │   ├── api/
│   │   │   ├── traffic.js
│   │   │   ├── intersections.js
│   │   │   ├── alerts.js
│   │   │   └── analytics.js
│   │   └── index.js
│   ├── services/
│   │   ├── dataAggregation.js
│   │   ├── realTimeProcessor.js
│   │   └── alertProcessor.js
│   ├── middleware/
│   │   ├── auth.js
│   │   ├── validation.js
│   │   └── rateLimiting.js
│   ├── utils/
│   │   ├── logger.js
│   │   └── helpers.js
│   └── app.js
├── package.json
├── Dockerfile
└── docker-compose.yml
```

### 2.2 Backend Package Configuration

**File**: `express-backend/package.json`

```json
{
  "name": "traffic-monitoring-backend",
  "version": "1.0.0",
  "description": "Traffic monitoring system backend API",
  "main": "src/app.js",
  "scripts": {
    "start": "node src/app.js",
    "dev": "nodemon src/app.js",
    "test": "jest",
    "lint": "eslint src/",
    "docker:build": "docker build -t traffic-backend .",
    "docker:run": "docker run -p 3000:3000 traffic-backend"
  },
  "dependencies": {
    "express": "^4.18.2",
    "kafkajs": "^2.2.4",
    "redis": "^4.6.7",
    "pg": "^8.11.0",
    "sequelize": "^6.32.1",
    "socket.io": "^4.7.2",
    "cors": "^2.8.5",
    "helmet": "^7.0.0",
    "express-rate-limit": "^6.8.1",
    "joi": "^17.9.2",
    "winston": "^3.10.0",
    "compression": "^1.7.4",
    "dotenv": "^16.3.1"
  },
  "devDependencies": {
    "nodemon": "^3.0.1",
    "jest": "^29.6.2",
    "eslint": "^8.45.0",
    "supertest": "^6.3.3"
  }
}
```

### 2.3 Kafka Consumer Implementation

**File**: `express-backend/src/consumers/trafficConsumer.js`

```javascript
const { Kafka } = require('kafkajs');
const { processTrafficData } = require('../services/dataAggregation');
const { emitRealTimeUpdate } = require('../services/realTimeProcessor');
const logger = require('../utils/logger');

class TrafficConsumer {
    constructor() {
        this.kafka = new Kafka({
            clientId: 'traffic-backend-consumer',
            brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
            retry: {
                initialRetryTime: 100,
                retries: 8
            }
        });
        
        this.consumer = this.kafka.consumer({ 
            groupId: 'traffic-backend-group',
            sessionTimeout: 30000,
            heartbeatInterval: 3000
        });
    }

    async start() {
        try {
            await this.consumer.connect();
            
            await this.consumer.subscribe({
                topics: [
                    'traffic-data',
                    'intersection-data',
                    'raw-vehicle-data',
                    'traffic-alerts',
                    'sensor-health'
                ],
                fromBeginning: false
            });

            await this.consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    try {
                        const data = JSON.parse(message.value.toString());
                        await this.processMessage(topic, data);
                    } catch (error) {
                        logger.error('Error processing message:', error);
                    }
                },
            });

            logger.info('Traffic consumer started successfully');
        } catch (error) {
            logger.error('Failed to start traffic consumer:', error);
            throw error;
        }
    }

    async processMessage(topic, data) {
        switch (topic) {
            case 'traffic-data':
                await this.handleTrafficData(data);
                break;
            case 'intersection-data':
                await this.handleIntersectionData(data);
                break;
            case 'raw-vehicle-data':
                await this.handleVehicleData(data);
                break;
            case 'traffic-alerts':
                await this.handleTrafficAlert(data);
                break;
            case 'sensor-health':
                await this.handleSensorHealth(data);
                break;
            default:
                logger.warn(`Unknown topic: ${topic}`);
        }
    }

    async handleTrafficData(data) {
        // Process and store traffic data
        const processedData = await processTrafficData(data);
        
        // Emit real-time update to dashboard
        emitRealTimeUpdate('traffic-update', processedData);
        
        // Store in cache for quick access
        await this.cacheTrafficData(processedData);
    }

    async handleIntersectionData(data) {
        // Process intersection data
        const processedData = await this.processIntersectionData(data);
        
        // Emit to dashboard
        emitRealTimeUpdate('intersection-update', processedData);
    }

    async handleVehicleData(data) {
        // Process individual vehicle data
        await this.aggregateVehicleData(data);
    }

    async handleTrafficAlert(data) {
        // Process alerts
        emitRealTimeUpdate('alert', data);
        
        // Store alert in database
        await this.storeAlert(data);
    }

    async handleSensorHealth(data) {
        // Monitor sensor health
        await this.updateSensorStatus(data);
        
        if (data.hw_fault || data.low_voltage) {
            emitRealTimeUpdate('sensor-alert', data);
        }
    }
}

module.exports = TrafficConsumer;
```

### 2.4 Real-Time Data Processing Service

**File**: `express-backend/src/services/realTimeProcessor.js`

```javascript
const socketIo = require('socket.io');
const redis = require('../config/redis');
const logger = require('../utils/logger');

class RealTimeProcessor {
    constructor() {
        this.io = null;
        this.connectedClients = new Map();
    }

    initialize(server) {
        this.io = socketIo(server, {
            cors: {
                origin: process.env.FRONTEND_URL || "http://localhost:3000",
                methods: ["GET", "POST"]
            }
        });

        this.io.on('connection', (socket) => {
            logger.info(`Client connected: ${socket.id}`);
            this.connectedClients.set(socket.id, socket);

            socket.on('subscribe-intersection', (intersectionId) => {
                socket.join(`intersection-${intersectionId}`);
            });

            socket.on('subscribe-alerts', () => {
                socket.join('alerts');
            });

            socket.on('disconnect', () => {
                logger.info(`Client disconnected: ${socket.id}`);
                this.connectedClients.delete(socket.id);
            });
        });
    }

    emitRealTimeUpdate(eventType, data) {
        if (!this.io) return;

        switch (eventType) {
            case 'traffic-update':
                this.io.emit('traffic-data', data);
                this.cacheUpdate('traffic', data);
                break;
            
            case 'intersection-update':
                this.io.to(`intersection-${data.intersection_id}`).emit('intersection-data', data);
                this.cacheUpdate('intersection', data);
                break;
            
            case 'alert':
                this.io.to('alerts').emit('traffic-alert', data);
                break;
            
            case 'sensor-alert':
                this.io.emit('sensor-status', data);
                break;
        }
    }

    async cacheUpdate(type, data) {
        try {
            const key = `${type}:${data.sensor_id || data.intersection_id}`;
            await redis.setex(key, 300, JSON.stringify(data)); // 5 minute cache
        } catch (error) {
            logger.error('Failed to cache update:', error);
        }
    }

    async getLatestData(type, id) {
        try {
            const key = `${type}:${id}`;
            const data = await redis.get(key);
            return data ? JSON.parse(data) : null;
        } catch (error) {
            logger.error('Failed to retrieve cached data:', error);
            return null;
        }
    }
}

module.exports = new RealTimeProcessor();
```

### 2.5 API Routes Implementation

**File**: `express-backend/src/routes/api/traffic.js`

```javascript
const express = require('express');
const router = express.Router();
const trafficController = require('../../controllers/trafficController');
const { validateQuery } = require('../../middleware/validation');

// GET /api/traffic/current
router.get('/current', trafficController.getCurrentTraffic);

// GET /api/traffic/intersection/:id
router.get('/intersection/:id', 
    validateQuery('intersectionQuery'),
    trafficController.getIntersectionTraffic
);

// GET /api/traffic/historical
router.get('/historical',
    validateQuery('timeRangeQuery'),
    trafficController.getHistoricalData
);

// GET /api/traffic/analytics/congestion
router.get('/analytics/congestion',
    validateQuery('analyticsQuery'),
    trafficController.getCongestionAnalytics
);

// GET /api/traffic/alerts
router.get('/alerts',
    validateQuery('alertQuery'),
    trafficController.getActiveAlerts
);

// WebSocket endpoint for real-time data
router.get('/stream', trafficController.getDataStream);

module.exports = router;
```

## Phase 3: NextJS Dashboard Implementation

### 3.1 Frontend Architecture

```
nextjs-dashboard/
├── src/
│   ├── app/
│   │   ├── dashboard/
│   │   │   ├── page.tsx
│   │   │   ├── traffic/
│   │   │   ├── intersections/
│   │   │   ├── alerts/
│   │   │   └── analytics/
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   └── globals.css
│   ├── components/
│   │   ├── ui/
│   │   │   ├── Button.tsx
│   │   │   ├── Card.tsx
│   │   │   └── Charts/
│   │   ├── dashboard/
│   │   │   ├── TrafficMap.tsx
│   │   │   ├── IntersectionWidget.tsx
│   │   │   ├── AlertPanel.tsx
│   │   │   └── MetricsCard.tsx
│   │   └── layout/
│   │       ├── Header.tsx
│   │       ├── Sidebar.tsx
│   │       └── Footer.tsx
│   ├── hooks/
│   │   ├── useRealTimeData.ts
│   │   ├── useTrafficData.ts
│   │   └── useWebSocket.ts
│   ├── lib/
│   │   ├── api.ts
│   │   ├── websocket.ts
│   │   └── utils.ts
│   ├── types/
│   │   ├── traffic.ts
│   │   ├── intersection.ts
│   │   └── api.ts
│   └── stores/
│       ├── trafficStore.ts
│       └── alertStore.ts
├── public/
├── package.json
└── next.config.js
```

### 3.2 Frontend Package Configuration

**File**: `nextjs-dashboard/package.json`

```json
{
  "name": "traffic-dashboard",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint",
    "type-check": "tsc --noEmit"
  },
  "dependencies": {
    "next": "14.0.0",
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "socket.io-client": "^4.7.2",
    "recharts": "^2.8.0",
    "leaflet": "^1.9.4",
    "react-leaflet": "^4.2.1",
    "zustand": "^4.4.1",
    "axios": "^1.5.0",
    "date-fns": "^2.30.0",
    "clsx": "^2.0.0",
    "tailwindcss": "^3.3.0",
    "framer-motion": "^10.16.4",
    "@radix-ui/react-select": "^1.2.2",
    "@radix-ui/react-dialog": "^1.0.5",
    "@radix-ui/react-alert-dialog": "^1.0.5"
  },
  "devDependencies": {
    "@types/node": "^20.6.0",
    "@types/react": "^18.2.21",
    "@types/react-dom": "^18.2.7",
    "@types/leaflet": "^1.9.4",
    "typescript": "^5.2.2",
    "eslint": "^8.49.0",
    "eslint-config-next": "14.0.0",
    "autoprefixer": "^10.4.15",
    "postcss": "^8.4.29"
  }
}
```

### 3.3 Real-Time Data Hook

**File**: `nextjs-dashboard/src/hooks/useRealTimeData.ts`

```typescript
import { useEffect, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import { TrafficData, IntersectionData, AlertData } from '@/types/traffic';

interface UseRealTimeDataProps {
  intersectionIds?: string[];
  subscribeToAlerts?: boolean;
}

export const useRealTimeData = ({ 
  intersectionIds = [], 
  subscribeToAlerts = true 
}: UseRealTimeDataProps = {}) => {
  const [socket, setSocket] = useState<Socket | null>(null);
  const [trafficData, setTrafficData] = useState<TrafficData[]>([]);
  const [intersectionData, setIntersectionData] = useState<Map<string, IntersectionData>>(new Map());
  const [alerts, setAlerts] = useState<AlertData[]>([]);
  const [connectionStatus, setConnectionStatus] = useState<'connected' | 'disconnected' | 'connecting'>('disconnected');

  useEffect(() => {
    const socketInstance = io(process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3001', {
      transports: ['websocket'],
    });

    setSocket(socketInstance);
    setConnectionStatus('connecting');

    socketInstance.on('connect', () => {
      setConnectionStatus('connected');
      
      // Subscribe to intersection data
      intersectionIds.forEach(id => {
        socketInstance.emit('subscribe-intersection', id);
      });
      
      // Subscribe to alerts if requested
      if (subscribeToAlerts) {
        socketInstance.emit('subscribe-alerts');
      }
    });

    socketInstance.on('disconnect', () => {
      setConnectionStatus('disconnected');
    });

    // Handle traffic data updates
    socketInstance.on('traffic-data', (data: TrafficData) => {
      setTrafficData(prev => {
        const updated = prev.filter(item => item.sensor_id !== data.sensor_id);
        return [...updated, data];
      });
    });

    // Handle intersection data updates
    socketInstance.on('intersection-data', (data: IntersectionData) => {
      setIntersectionData(prev => {
        const updated = new Map(prev);
        updated.set(data.intersection_id, data);
        return updated;
      });
    });

    // Handle alerts
    socketInstance.on('traffic-alert', (alert: AlertData) => {
      setAlerts(prev => [alert, ...prev.slice(0, 49)]); // Keep latest 50 alerts
    });

    socketInstance.on('sensor-status', (status: any) => {
      // Handle sensor status updates
      console.log('Sensor status update:', status);
    });

    return () => {
      socketInstance.disconnect();
    };
  }, [intersectionIds, subscribeToAlerts]);

  const subscribeToIntersection = (intersectionId: string) => {
    if (socket && connectionStatus === 'connected') {
      socket.emit('subscribe-intersection', intersectionId);
    }
  };

  const unsubscribeFromIntersection = (intersectionId: string) => {
    if (socket && connectionStatus === 'connected') {
      socket.emit('unsubscribe-intersection', intersectionId);
    }
  };

  return {
    trafficData,
    intersectionData: Array.from(intersectionData.values()),
    alerts,
    connectionStatus,
    subscribeToIntersection,
    unsubscribeFromIntersection,
  };
};
```

### 3.4 Traffic Dashboard Component

**File**: `nextjs-dashboard/src/components/dashboard/TrafficDashboard.tsx`

```typescript
'use client';

import React, { useState, useEffect } from 'react';
import { useRealTimeData } from '@/hooks/useRealTimeData';
import { TrafficMap } from './TrafficMap';
import { IntersectionWidget } from './IntersectionWidget';
import { AlertPanel } from './AlertPanel';
import { MetricsCard } from './MetricsCard';
import { Card } from '@/components/ui/Card';

export const TrafficDashboard: React.FC = () => {
  const [selectedIntersections, setSelectedIntersections] = useState(['bd-anfa-bd-zerktouni']);
  
  const {
    trafficData,
    intersectionData,
    alerts,
    connectionStatus,
  } = useRealTimeData({
    intersectionIds: selectedIntersections,
    subscribeToAlerts: true,
  });

  const activeAlerts = alerts.filter(alert => 
    Date.now() - new Date(alert.timestamp).getTime() < 300000 // 5 minutes
  );

  // Calculate aggregate metrics
  const totalVehicles = trafficData.reduce((sum, data) => sum + data.vehicle_number, 0);
  const averageSpeed = trafficData.length > 0 
    ? trafficData.reduce((sum, data) => sum + data.speed, 0) / trafficData.length 
    : 0;
  const congestionLevel = trafficData.length > 0
    ? trafficData.filter(data => data.congestion_level === 'high').length / trafficData.length
    : 0;

  return (
    <div className="p-6 space-y-6">
      {/* Header with connection status */}
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold">Traffic Control Dashboard</h1>
        <div className={`px-3 py-1 rounded-full text-sm font-medium ${
          connectionStatus === 'connected' 
            ? 'bg-green-100 text-green-800' 
            : connectionStatus === 'connecting'
            ? 'bg-yellow-100 text-yellow-800'
            : 'bg-red-100 text-red-800'
        }`}>
          {connectionStatus === 'connected' ? '🟢 Live' : 
           connectionStatus === 'connecting' ? '🟡 Connecting' : '🔴 Offline'}
        </div>
      </div>

      {/* Key Metrics Row */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <MetricsCard
          title="Total Vehicles"
          value={totalVehicles.toString()}
          trend={"+12%"}
          positive={true}
        />
        <MetricsCard
          title="Average Speed"
          value={`${Math.round(averageSpeed)} km/h`}
          trend={"-3%"}
          positive={false}
        />
        <MetricsCard
          title="Congestion Rate"
          value={`${Math.round(congestionLevel * 100)}%`}
          trend={"+8%"}
          positive={false}
        />
        <MetricsCard
          title="Active Alerts"
          value={activeAlerts.length.toString()}
          trend={activeAlerts.length > 0 ? "New" : "None"}
          positive={activeAlerts.length === 0}
        />
      </div>

      {/* Main Dashboard Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Traffic Map - Takes 2 columns */}
        <div className="lg:col-span-2">
          <Card>
            <div className="p-4">
              <h2 className="text-xl font-semibold mb-4">Live Traffic Map</h2>
              <TrafficMap 
                trafficData={trafficData}
                intersectionData={intersectionData}
                alerts={activeAlerts}
              />
            </div>
          </Card>
        </div>

        {/* Alert Panel - Takes 1 column */}
        <div>
          <AlertPanel alerts={alerts} />
        </div>
      </div>

      {/* Intersection Widgets */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {intersectionData.map(intersection => (
          <IntersectionWidget
            key={intersection.intersection_id}
            intersectionData={intersection}
            trafficData={trafficData.filter(data => 
              data.sensor_id.includes(intersection.intersection_id)
            )}
          />
        ))}
      </div>
    </div>
  );
};
```

## Phase 4: Deployment and DevOps

### 4.1 Production Docker Compose

**File**: `docker-compose.prod.yml`

```yaml
version: '3.8'
services:
  # Infrastructure
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
    restart: unless-stopped

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'
    volumes:
      - kafka-data:/var/lib/kafka/data
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data
    restart: unless-stopped

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - postgres-data:/var/lib/postgresql/data
    restart: unless-stopped

  # Applications
  traffic-simulator:
    build:
      context: ./rust-simulator
      dockerfile: Dockerfile
    depends_on:
      - kafka
    environment:
      KAFKA_BROKERS: kafka:29092
    restart: unless-stopped

  backend:
    build:
      context: ./express-backend
      dockerfile: Dockerfile
    depends_on:
      - kafka
      - redis
      - postgres
    environment:
      KAFKA_BROKER: kafka:29092
      REDIS_URL: redis://redis:6379
      DATABASE_URL: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
      NODE_ENV: production
    ports:
      - "3001:3001"
    restart: unless-stopped

  frontend:
    build:
      context: ./nextjs-dashboard
      dockerfile: Dockerfile
    environment:
      NEXT_PUBLIC_API_URL: http://backend:3001
    ports:
      - "3000:3000"
    restart: unless-stopped

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - frontend
      - backend
    restart: unless-stopped

volumes:
  zookeeper-data:
  kafka-data:
  redis-data:
  postgres-data:
```

### 4.2 Monitoring and Health Checks

**File**: `monitoring/docker-compose.monitoring.yml`

```yaml
version: '3.8'
services:
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus

  grafana:
    image: grafana/grafana
    ports:
      - "3001:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources

volumes:
  prometheus-data:
  grafana-data:
```

## Phase 5: Testing and Validation

### 5.1 Integration Testing Strategy

```bash
# Test pipeline end-to-end
./scripts/test-pipeline.sh

# Load testing
./scripts/load-test.sh

# Data consistency validation
./scripts/validate-data-consistency.sh
```

### 5.2 Performance Benchmarks

- **Kafka Throughput**: 10,000+ messages/second
- **API Response Time**: <200ms for 95th percentile
- **Dashboard Update Latency**: <500ms from sensor to UI
- **Data Consistency**: 99.9% accuracy between adjacent sensors

## Implementation Timeline

| Phase | Duration | Deliverables |
|-------|----------|-------------|
| Phase 1: Kafka Enhancement | 1 week | Enhanced Kafka setup, topic creation |
| Phase 2: Backend Development | 2 weeks | Express.js API, real-time processing |
| Phase 3: Frontend Development | 2 weeks | NextJS dashboard, real-time updates |
| Phase 4: Integration & Testing | 1 week | End-to-end testing, performance tuning |
| Phase 5: Deployment & Monitoring | 1 week | Production deployment, monitoring setup |

**Total Timeline**: 7 weeks

This integration architecture ensures a robust, scalable, and real-time traffic monitoring system from simulator to dashboard, Big Daddy. 