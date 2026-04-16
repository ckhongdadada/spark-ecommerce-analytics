"""
Atlas 元数据管理集成
注册数据资产到 Apache Atlas
"""
import requests
import json
import sys


ATLAS_URL = "http://atlas:21000/api/atlas/v2"
ATLAS_USER = "admin"
ATLAS_PASSWORD = "admin"


def get_auth():
    return (ATLAS_USER, ATLAS_PASSWORD)


def create_typedefs():
    typedefs = {
        "enumDefinitions": [],
        "structDefinitions": [],
        "classificationDefinitions": [],
        "entityDefinitions": [
            {
                "name": "ecommerce_table",
                "superTypes": ["DataSet"],
                "attributeDefinitions": [
                    {"name": "layer", "typeName": "string", "cardinality": "SINGLE", "isOptional": False},
                    {"name": "database", "typeName": "string", "cardinality": "SINGLE", "isOptional": False},
                    {"name": "table_name", "typeName": "string", "cardinality": "SINGLE", "isOptional": False},
                    {"name": "description", "typeName": "string", "cardinality": "SINGLE", "isOptional": True},
                    {"name": "owner", "typeName": "string", "cardinality": "SINGLE", "isOptional": True},
                    {"name": "refresh_frequency", "typeName": "string", "cardinality": "SINGLE", "isOptional": True},
                ]
            }
        ],
        "relationshipDefinitions": []
    }

    try:
        resp = requests.post(
            f"{ATLAS_URL}/types/typedefs",
            auth=get_auth(),
            headers={"Content-Type": "application/json"},
            json=typedefs,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            print("Atlas typedefs created successfully")
        elif resp.status_code == 409:
            print("Atlas typedefs already exist")
        else:
            print(f"Atlas typedefs creation failed: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Atlas typedefs creation error: {e}")


def register_entity(layer, database, table_name, description="", owner="ecommerce-platform", refresh_frequency="daily"):
    entity = {
        "entity": {
            "typeName": "ecommerce_table",
            "attributes": {
                "name": f"{database}.{table_name}",
                "qualifiedName": f"{database}.{table_name}@ecommerce",
                "layer": layer,
                "database": database,
                "table_name": table_name,
                "description": description,
                "owner": owner,
                "refresh_frequency": refresh_frequency,
            }
        }
    }

    try:
        resp = requests.post(
            f"{ATLAS_URL}/entity",
            auth=get_auth(),
            headers={"Content-Type": "application/json"},
            json=entity,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            print(f"Registered: {database}.{table_name}")
        else:
            print(f"Failed to register {database}.{table_name}: {resp.status_code}")
    except Exception as e:
        print(f"Error registering {database}.{table_name}: {e}")


def register_all_entities():
    tables = [
        ("ODS", "ecommerce", "ods_user_behavior", "原始用户行为数据", "realtime"),
        ("DWD", "ecommerce", "dwd_user_behavior_detail", "清洗后用户行为明细", "daily"),
        ("DWD", "ecommerce", "dwd_user_profile", "用户画像", "daily"),
        ("DWD", "ecommerce", "dwd_item_profile", "商品画像", "daily"),
        ("DWS", "ecommerce", "dws_user_daily", "用户日汇总", "daily"),
        ("DWS", "ecommerce", "dws_item_daily", "商品日汇总", "daily"),
        ("DWS", "ecommerce", "dws_category_daily", "类目日汇总", "daily"),
        ("ADS", "ecommerce", "ads_user_metrics", "用户指标", "daily"),
        ("ADS", "ecommerce", "ads_item_metrics", "商品指标", "daily"),
        ("ADS", "ecommerce", "ads_category_metrics", "类目指标", "daily"),
        ("REALTIME", "ecommerce", "realtime_category_metrics", "实时类目指标", "realtime"),
        ("REALTIME", "ecommerce", "realtime_order_metrics", "实时订单指标", "realtime"),
        ("ALERT", "ecommerce", "alert_events", "告警事件", "realtime"),
    ]

    create_typedefs()

    for layer, db, table, desc, freq in tables:
        register_entity(layer, db, table, desc, refresh_frequency=freq)

    print(f"\nRegistered {len(tables)} entities in Atlas")


if __name__ == "__main__":
    register_all_entities()
