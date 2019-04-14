package main

import (
	"encoding/json"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/pkg/errors"
	redisenterprise "github.com/zlangbert/redis-enterprise-client-go"
	"log"
	"strconv"
)

func resourceDatabase() *schema.Resource {
	return &schema.Resource{
		Exists: resourceDatabaseExists,
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
				Required: true,
			},
			"replication": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"sharding": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"enabled": {
							Type:     schema.TypeBool,
							Computed: true,
						},
						"shards": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  1,
						},
						"placement": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "dense",
						},
					},
				},
				Default: map[string]interface{}{
					"count":     1,
					"placement": "dense",
				},
			},
		},
	}
}

func resourceDatabaseExists(d *schema.ResourceData, m interface{}) (bool, error) {
	meta := m.(*providerMeta)
	id, _ := strconv.Atoi(d.Id())

	log.Printf("[DEBUG] checking for existence of database %v", id)
	_, response, err := meta.client.DatabasesApi.GetDatabase(meta.ctx, int32(id))

	if response.StatusCode == 200 {
		return true, nil
	}

	if response.StatusCode == 404 {
		return false, nil
	}

	return false, getClientError(err)
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
		db.MemorySize = int64(v.(int))
	}

	if v, ok := d.GetOk("replication"); ok {
		db.Replication = v.(bool)
	}

	if v, ok := d.GetOk("sharding"); ok {
		shardingConfig := v.(map[string]interface{})

		shards, err := strconv.Atoi(shardingConfig["shards"].(string))
		if err != nil {
			return errors.Wrapf(err, "failed to parse sharding.shards value: %v", shardingConfig["shards"])
		}

		if shards > 1 {
			db.Sharding = true
		}
		db.ShardsCount = int32(shards)
		db.ShardsPlacement = shardingConfig["placement"].(string)
	}

	payload, _ := json.Marshal(db)
	log.Printf("[DEBUG] creating database with payload: %s", payload)

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

	log.Printf("[DEBUG] read database %v: %#v", id, database)

	_ = d.Set("name", database.Name)
	_ = d.Set("type", database.Type)
	_ = d.Set("port", database.Port)
	_ = d.Set("memory_size", database.MemorySize)
	_ = d.Set("replication", database.Replication)

	_ = d.Set("sharding", map[string]interface{}{
		"enabled":   database.Sharding,
		"shards":    int(database.ShardsCount),
		"placement": database.ShardsPlacement,
	})

	return nil
}

func resourceDatabaseUpdate(d *schema.ResourceData, m interface{}) error {
	meta := m.(*providerMeta)
	id, _ := strconv.Atoi(d.Id())

	db := redisenterprise.Database{}

	if d.HasChange("name") {
		db.Name = d.Get("name").(string)
	}

	if d.HasChange("memory_size") {
		db.MemorySize = int64(d.Get("memory_size").(int))
	}

	if d.HasChange("replication") {
		db.Replication = d.Get("replication").(bool)
	}

	payload, _ := json.Marshal(db)
	log.Printf("[DEBUG] updating database with payload: %s", payload)

	_, _, err := meta.client.DatabasesApi.UpdateDatabase(meta.ctx, int32(id), db)
	if err != nil {
		return errors.Wrapf(getClientError(err), "error updating database %v", id)
	}

	return resourceDatabaseRead(d, m)
}

func resourceDatabaseDelete(d *schema.ResourceData, m interface{}) error {
	meta := m.(*providerMeta)
	id, _ := strconv.Atoi(d.Id())

	_, err := meta.client.DatabasesApi.DeleteDatabase(meta.ctx, int32(id))
	if err != nil {
		return errors.Wrapf(getClientError(err), "error deleting database %v", id)
	}

	return nil
}

func getClientError(err error) error {

	log.Printf("[DEBUG] unwraping client error: %#v", err)

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
