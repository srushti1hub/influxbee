## Step 1. Create Go Module
```
mkdir -p influxdb_go_client
cd influxdb_go_client
go mod init influxdb_go_client
touch main.go
```

## Step 2. Install influxdb-client-go 
```
go get github.com/influxdata/influxdb-client-go/v2
```

## Step 3. Install flight-sql
```
go get github.com/apache/arrow/go/v12/arrow/flight/flightsql
```

## Step 4. Token as env var
InfluxDB Cloud uses Tokens to authenticate API access. 
```
export <your_token>
```
