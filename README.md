# Speedometer Filter Plugin for Embulk

[![Build Status](https://travis-ci.org/hata/embulk-filter-speedometer.svg?branch=master)](https://travis-ci.org/hata/embulk-filter-speedometer.svg?branch=master)

Write log message of processed bytes and throughput periodically.
This plugin works with embulk:0.8.0 or later versions.

## Overview

- **Plugin type**: filter

## Configuration

- **log_interval_seconds**: Interval seconds to write log message periodically. (integer, optional, default: 10). If this value is set to 0, then interval message is not shown. It only show message when there is no active thread.
- **speed_limit**: Set maximum processing size per second. If 0 is set, then no limit. (integer, optional, default: 0)
- **delimiter**: Delimiter text to calculate delimiter length. (string, optional, default: ",")
- **record_padding_size**: Additional byte size for each record like a return code length. (integer, optional, default: 1)
- **column_options**: A map whose keys are name of columns like csv formatter plugin (hash, optional, default: {})

## Example of Configuration

- Use default parameters. Log message is output every 10 seconds.

```yaml
filters:
  - type: speedometer
```

- Change log message interval from 10 seconds(default) to 20 seconds.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
```

- If it is required to set a speed limit for throughput in filter, then set bytes per second to **speed_limit** parameter. If it is not required, then set 0(default). The following example is to set 250kbytes per second. This is all thread's total speed limit.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
    speed_limit: 250000
```

- If it is required to change delimiter size and record padding size, then set **delimiter** and **record_padding_size** . Set text for **delimiter** to calculate length. Set integer to **record_padding_size** to add a length for each record. It is like return code length for each line.

```yaml
filters:
  - type: speedometer
    log_interval_seconds: 20
    speed_limit: 250000
    delimiter: ", "
    record_padding_size: 0
```

- Set timestamp format to change default format for timestamp columns. The following **column_options** set %Y-%m-%d %H:%M:%S for time column and %Y%m%d for purchase column. If this option is not set, then a default embulk timestamp format is used.

```yaml
filters:
  - type: speedometer
    speed_limit: 250000
    column_options:
      time: {format: '%Y-%m-%d %H:%M:%S'}
      purchase: {format: '%Y%m%d'}
```


## Sample Log Message

```
2015-11-27 13:43:25.592 +0900 [INFO] (task-0006): {speedometer: {active: 4, total: 13.5mb, sec: 1:51, speed: 121kb/s, records: 269,748, record-speed: 2,435/s}}
2015-11-27 13:43:35.642 +0900 [INFO] (task-0004): {speedometer: {active: 4, total: 14.9mb, sec: 2:01, speed: 141kb/s, records: 297,532, record-speed: 2,832/s}}
2015-11-27 13:43:45.661 +0900 [INFO] (task-0005): {speedometer: {active: 3, total: 16.5mb, sec: 2:11, speed: 119kb/s, records: 329,484, record-speed: 2,395/s}}
```

- **active**: running threads
- **total**: processed bytes. This size is calculated based on text data like csv. For example, boolean value is 4 bytes(true) or 5 bytes(false). The default configuration set a byte for delimiter and a byte padding for each record.
- **sec**: elapsed time.
- **speed**: bytes per second.
- **records**: total processed records.
- **record-speed**: total processed records per second.

## Build

```
$ ./gradlew gem
```

Build with integrationTest

```
$ ./gradlew -DenableIntegrationTest=true gem
```


## Note

The shown data is caled based on text data size while using this filter plugin. So, the data is not the same data as read bytes and write bytes n input and output plugins. And this plugin has a little overhead to measure the bytes.


