package main

import (
	"context"
	"github.com/hashicorp/terraform/helper/schema"
	redis "github.com/zlangbert/redis-enterprise-client-go"
)

type providerMeta struct {
	client redis.APIClient
	ctx    context.Context
}

func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"base_url": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Base url of the Redis Enterprise cluster",
			},
			"username": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Username used to authenticate with the management api",
			},
			"password": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "Password used to authenticate with the management api",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"redisenterprise_database": resourceDatabase(),
		},

		ConfigureFunc: func(data *schema.ResourceData) (meta interface{}, e error) {

			config := redis.NewConfiguration()
			config.BasePath = data.Get("base_url").(string)

			client := redis.NewAPIClient(config)

			ctx := context.Background()
			ctx = context.WithValue(ctx,
				redis.ContextBasicAuth,
				redis.BasicAuth{
					UserName: data.Get("username").(string),
					Password: data.Get("password").(string),
				},
			)

			return &providerMeta{
				client: *client,
				ctx:    ctx,
			}, nil
		},
	}
}
