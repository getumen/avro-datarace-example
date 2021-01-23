module github.com/getumen/avro-datarace-example

go 1.15

require (
	github.com/hamba/avro v1.5.1
	github.com/linkedin/goavro v2.1.0+incompatible
	gopkg.in/linkedin/goavro.v1 v1.0.5 // indirect
)

replace github.com/hamba/avro v1.5.1 => ../avro
