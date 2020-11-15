# DynamoDB
- DynamoDB Client Utilities

## Installation

Please make sure to initialize a Go module before installing common-go/dynamodb:

```shell
go get -u github.com/common-go/dynamodb
```

Import:

```go
import "github.com/common-go/dynamodb"
```

You can optimize the import by version:
- v0.0.1: Utilities to support query, find one by Id
- v0.0.4: Utilities to support insert, update, patch, upsert, delete
- v0.0.7: Utilities to support batch update
- v0.1.0: FieldLoader, ViewService and GenericService
- v0.1.1: SearchService