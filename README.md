# cloudantbackup

A backup utility for Cloudant databases.

## Installation

You will need to [download and install the Go compiler](https://go.dev/doc/install). Clone this repo then:

```sh
go build
```

Then copy the resultant binary `cloudantbackup` (or `cloudantbackup.exe` in Windows systems) into your path.

## Configuration

`cloudantbackup` authenticates with your chosen Cloudant service using environment variables as documented [here](https://github.com/IBM/cloudant-go-sdk/blob/v0.10.8/docs/Authentication.md#authentication-with-environment-variables) e.g.

```sh
CLOUDANT_URL=https://xxxyyy.cloudantnosqldb.appdomain.cloud
CLOUDANT_APIKEY="my_api_key"
```

## Usage

Supply the name of the database to be backed up and pipe the output to a file:

```sh
cloudantbackup --db mydb > mydb.txt
```

By default, only 5 API calls can be in flight at any one time. This can be increased or decreased with the `--parallelism`/`--p` option

```sh
# import data with a maximum of 5 bulk write API calls in flight at once
cloudantbackup --db mydb --parallelism 2 > mydb.txt
```

## How does it work?

To remind myself of what's going on, this diagram helps:

![diagram](cloudantbackup.png)
