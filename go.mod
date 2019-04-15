module github.com/zlangbert/terraform-provider-redis-enterprise

go 1.12

require (
	github.com/hashicorp/terraform v0.11.13
	github.com/pkg/errors v0.8.1
	github.com/zlangbert/redis-enterprise-client-go v0.0.0
)

replace github.com/zlangbert/redis-enterprise-client-go => ../redis-enterprise-client-go
