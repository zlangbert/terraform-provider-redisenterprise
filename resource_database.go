package main

import (
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/pkg/errors"
	redisenterprise "github.com/zlangbert/redis-enterprise-client-go"
	"log"
	"strconv"
)

func resourceDatabase() *schema.Resource {
	return &schema.Resource{
		Create: resourceDatabaseCreate,
		Read:   resourceDatabaseRead,
		Update: resourceDatabaseUpdate,
		Delete: resourceDatabaseDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"type": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"port": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"memory_size": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"shard_count": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  1,
			},
		},
	}
}

func resourceDatabaseCreate(d *schema.ResourceData, m interface{}) error {
	meta := m.(*providerMeta)

	db := redisenterprise.Database{
		Name: d.Get("name").(string),
		Type: d.Get("type").(string),
	}

	if v, ok := d.GetOk("port"); ok {
		db.Port = int32(v.(int))
	}

	if v, ok := d.GetOk("memory_size"); ok {
		db.MemorySize = int32(v.(int))
	} else {
		db.MemorySize = 0
	}

	if v, ok := d.GetOk("shard_count"); ok {
		db.ShardsCount = int32(v.(int))
	}

	log.Printf("[DEBUG] creating database with payload: %#v", db)

	database, _, err := meta.client.DatabasesApi.CreateDatabase(meta.ctx, db)
	if err != nil {
		return errors.Wrapf(getClientError(err), "error creating database")
	}

	log.Printf("[DEBUG] created database: %#v", database)

	d.SetId(strconv.Itoa(int(database.Uid)))

	return resourceDatabaseRead(d, m)
}

func resourceDatabaseRead(d *schema.ResourceData, m interface{}) error {
	meta := m.(*providerMeta)

	id, _ := strconv.Atoi(d.Id())

	database, _, err := meta.client.DatabasesApi.GetDatabase(meta.ctx, int32(id))
	if err != nil {
		return errors.Wrapf(getClientError(err), "error getting database %v", id)
	}

	_ = d.Set("name", database.Name)
	_ = d.Set("type", database.Type)
	_ = d.Set("port", database.Port)
	_ = d.Set("memory_size", database.MemorySize)
	_ = d.Set("shard_count", database.ShardsCount)

	return nil
}

func resourceDatabaseUpdate(d *schema.ResourceData, m interface{}) error {
	return resourceDatabaseRead(d, m)
}

func resourceDatabaseDelete(d *schema.ResourceData, m interface{}) error {
	return nil
}

func getClientError(err error) error {

	switch err := err.(type) {
	default:
		return errors.New("unknown error")
	case redisenterprise.GenericOpenAPIError:

		switch serviceErr := err.Model().(type) {
		default:
			return errors.New(err.Error())
		case redisenterprise.Error:
			if serviceErr.Description != "" {
				return errors.New(serviceErr.Description)
			} else if serviceErr.ErrorCode != "" {
				return errors.New(serviceErr.ErrorCode)
			} else {
				return errors.New(err.Error())
			}
		}
	}
}
