package main

import (
  "context"
  "crypto/x509"
  "encoding/json"
  "fmt"
  "os"
  "time"

  "github.com/apache/arrow/go/v12/arrow/flight/flightsql"
  influxdb2 "github.com/influxdata/influxdb-client-go/v2"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials"
  "google.golang.org/grpc/metadata"
)

func main() {
  if err := dbQuery(context.Background()); err != nil {
    fmt.Fprintf(os.Stderr, "error: %v\n", err)
    os.Exit(1)
  }
}

func dbWrite(ctx context.Context) error {
    // Create write client
    url := "https://us-east-1-1.aws.cloud2.influxdata.com"
    token := os.Getenv("INFLUXDB_TOKEN")
    writeClient := influxdb2.NewClient(url, token)

    // Define write API
    org := "IIITV"
    bucket := "LOL"
    writeAPI := writeClient.WriteAPIBlocking(org, bucket)
	data := map[string]map[string]interface{}{
  "point1": {
    "location": "Klamath",
    "species":  "bees",
    "count":    23,
  },
  "point2": {
    "location": "Portland",
    "species":  "ants",
    "count":    30,
  },
  "point3": {
    "location": "Klamath",
    "species":  "bees",
    "count":    28,
  },
  "point4": {
    "location": "Portland",
    "species":  "ants",
    "count":    32,
  },
  "point5": {
    "location": "Klamath",
    "species":  "bees",
    "count":    29,
  },
  "point6": {
    "location": "Portland",
    "species":  "ants",
    "count":    40,
  },
}

// Write data
for key := range data {
  point := influxdb2.NewPointWithMeasurement("census").
    AddTag("location", data[key]["location"].(string)).
    AddField(data[key]["species"].(string), data[key]["count"])

  if err := writeAPI.WritePoint(ctx, point); err != nil {
    return fmt.Errorf("write API write point: %s", err)
  }

  time.Sleep(1 * time.Second) // separate points by 1 second
}

return nil
}

func dbQuery(ctx context.Context) error {
  url := "us-east-1-1.aws.cloud2.influxdata.com:443"
  token := os.Getenv("INFLUXDB_TOKEN")
  bucket := "LOL"

  // Create a gRPC transport
  pool, err := x509.SystemCertPool()
  if err != nil {
    return fmt.Errorf("x509: %s", err)
  }
  transport := grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pool, ""))
  opts := []grpc.DialOption{
    transport,
  }

  // Create query client
  client, err := flightsql.NewClient(url, nil, nil, opts...)
  if err != nil {
    return fmt.Errorf("flightsql: %s", err)
  }

  ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
  ctx = metadata.AppendToOutgoingContext(ctx, "bucket-name", bucket)

  // Execute query
query := `SELECT *
FROM 'census'
WHERE time >= now() - interval '1 hour'
  AND ('bees' IS NOT NULL OR 'ants' IS NOT NULL)`

info, err := client.Execute(ctx, query)
if err != nil {
return fmt.Errorf("flightsql flight info: %s", err)
}
reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
if err != nil {
return fmt.Errorf("flightsql do get: %s", err)
}

// Print results as JSON
for reader.Next() {
record := reader.Record()
b, err := json.MarshalIndent(record, "", "  ")
if err != nil {
return err
}
fmt.Println("RECORD BATCH")
fmt.Println(string(b))

if err := reader.Err(); err != nil {
return fmt.Errorf("flightsql reader: %s", err)
}
}

return nil
}