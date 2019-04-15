package main

import (
	"encoding/json"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"github.com/pkg/errors"
	redisenterprise "github.com/zlangbert/redis-enterprise-client-go"
	"log"
	"regexp"
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
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"shard_count": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      1,
				ValidateFunc: validation.IntBetween(1, 512),
			},
			"shard_placement": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "dense",
				ValidateFunc: validation.StringMatch(
					regexp.MustCompile(`dense|sparse`),
					"shard placement policy should be one of 'dense' or 'sparse'",
				),
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
		db.Sharding = v.(bool)
	}

	if v, ok := d.GetOk("shard_count"); ok {
		db.ShardsCount = int32(v.(int))
	}

	if v, ok := d.GetOk("shard_placement"); ok {
		db.ShardsPlacement = v.(string)
	}

	payload, _ := json.Marshal(db)
	log.Printf("[DEBUG] creating database with payload: %s", payload)

	database, _, err := meta.client.DatabasesApi.CreateDatabase(meta.ctx, db)
	if err != nil {
		return errors.Wrapf(getClientError(err), "error creating database")
	}

	log.Printf("[DEBUG] created database: %#v", database)
	d.SetId(strconv.Itoa(int(database.Uid)))

	stateConf := resource.StateChangeConf{
		Pending: []string{string(redisenterprise.PENDING)},
		Target:  []string{string(redisenterprise.ACTIVE)},
		Timeout: d.Timeout(schema.TimeoutCreate),
		Refresh: refreshDatabaseStatus(meta, database.Uid),
	}
	_, err = stateConf.WaitForState()
	if err != nil {
		return err
	}

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
	_ = d.Set("sharding", database.Sharding)
	_ = d.Set("shard_count", database.ShardsCount)
	_ = d.Set("shard_placement", database.ShardsPlacement)

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

	if d.HasChange("sharding") {
		db.Sharding = d.Get("sharding").(bool)
	}

	if d.HasChange("shard_count") {
		db.ShardsCount = int32(d.Get("shard_count").(int))
	}

	if d.HasChange("shard_placement") {
		db.ShardsPlacement = d.Get("shard_count").(string)
	}

	payload, _ := json.Marshal(db)
	log.Printf("[DEBUG] updating database with payload: %s", payload)

	_, _, err := meta.client.DatabasesApi.UpdateDatabase(meta.ctx, int32(id), db)
	if err != nil {
		return errors.Wrapf(getClientError(err), "error updating database %v", id)
	}

	stateConf := resource.StateChangeConf{
		Pending: []string{string(redisenterprise.ACTIVE_CHANGE_PENDING)},
		Target:  []string{string(redisenterprise.ACTIVE)},
		Timeout: d.Timeout(schema.TimeoutUpdate),
		Refresh: refreshDatabaseStatus(meta, int32(id)),
	}
	_, err = stateConf.WaitForState()
	if err != nil {
		return err
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

func refreshDatabaseStatus(meta *providerMeta, id int32) resource.StateRefreshFunc {
	return func() (interface{}, string, error) {
		database, _, err := meta.client.DatabasesApi.GetDatabase(meta.ctx, id)
		if err != nil {
			return 0, "", errors.Wrap(err, "error getting database during status refresh")
		}
		return database, string(database.Status), nil
	}
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
