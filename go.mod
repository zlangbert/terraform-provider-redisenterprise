module github.com/zlangbert/terraform-provider-redis-enterprise

go 1.12

require github.com/hashicorp/terraform v0.11.13

require (
	github.com/pkg/errors v0.8.1
	github.com/zlangbert/redis-enterprise-client-go v0.0.0
	golang.org/x/net v0.0.0-20171004034648-a04bdaca5b32
)

replace github.com/zlangbert/redis-enterprise-client-go => ../redis-enterprise-client-go
