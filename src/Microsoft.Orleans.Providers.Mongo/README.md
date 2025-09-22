## Microsoft.Orleans.Providers.Mongo

This project is a fork of [OrleansContrib/Orleans.Providers.MongoDB](https://github.com/OrleansContrib/Orleans.Providers.MongoDB).

### Purpose
It provides MongoDB-based storage providers for Microsoft Orleans, enabling the use of MongoDB as a backend for grain state, reminders, and event sourcing in distributed applications built with Orleans.

### Key Features
- **Grain Storage Provider:** Store and retrieve grain state in MongoDB.
- **Reminder Provider:** Persist Orleans reminders in MongoDB.
- **Event Sourcing Support:** (if applicable) Store events for grains using MongoDB.
- **Custom Enhancements:** This fork may include performance improvements, bug fixes, or additional features tailored for the Mailbiz.One.Journey.Flows solution.

### Getting Started
1. Reference this project in your Orleans solution.
2. Configure the MongoDB provider in your Orleans silo configuration (see the original [OrleansContrib documentation](https://github.com/OrleansContrib/Orleans.Providers.MongoDB) for setup details).
3. Adjust connection strings and options as needed for your environment.

### Differences from Upstream
This fork includes several changes and enhancements compared to the upstream project:

- **Targeted for .NET 9.0:** Updated to support the latest .NET and Orleans SDK versions.
- **Custom Client Factory:** Implements a flexible `IMongoClientFactory` and related utilities for improved MongoDB client management and dependency injection.
- **Enhanced Configuration:** Extension methods for silo and client builders allow easy configuration of MongoDB providers using connection strings or custom settings.
- **Collection Prefix Support:** Adds support for custom collection prefixes for multi-tenancy and environment separation.
- **Serializer Improvements:** Custom grain state serializers and options (e.g., `JsonGrainStateSerializerOptions`) for better compatibility and performance.
- **Performance and Reliability:** Includes optimizations, bug fixes, and reliability improvements beyond the upstream version.
- **Mailbiz-Specific Features:** Tailored enhancements for Mailbiz.One.Journey.Flows, such as improved error handling, configuration, and integration patterns.

**Additional Change:**
- **FieldGrainId:** The `MongoGrainStorageCollection` class introduces a new field, `FieldGrainId`, which stores the grain ID in each document. This provides easier querying and debugging of grain state records, and is not present in the upstream implementation.

For a detailed changelog or specific commit history, review the repository or contact the maintainers.

### License
This project inherits the license from the upstream repository. See the LICENSE file for details.
