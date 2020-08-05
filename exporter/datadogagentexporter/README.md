# DataDog Exporter

This exporter supports sending trace data to [DataDog](https://www.datadog.com)

The following configuration options are supported:

* `access_token` (Required): The access token for your DataDog project.
* `satellite_host` (Optional): Your DataDog Satellite Pool Hostname. Defaults to `ingest.datadog.com`.
* `satellite_port` (Optional): Your DataDog Satellite Pool Port. Defaults to `443`.
* `service_name` (Optional): The service name for spans reported by this collector. Defaults to `opentelemetry-collector`. 
* `plain_text` (Optional): False for HTTPS DataDog Satellite Pools. Defaults to `False`.
Example:

```yaml
exporters:
    datadog:
        access_token: "abcdef12345"
        satellite_host: "my.satellite.pool.coolcat.com"
        satellite_port: 8000
        service_name: "myService"
        plain_text: true
```
