"""Provision a connection, dataset and simple dashboard without UI clicks."""

import os
from urllib.parse import quote

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.slice import Slice
    from superset.models.dashboard import Dashboard
    import json

    from superset import appbuilder

    manager = appbuilder.sm
    if not manager.find_user(username=os.environ["SUPERSET_ADMIN_USER"]):
        user = manager.add_user(
            username=os.environ["SUPERSET_ADMIN_USER"],
            first_name="Local",
            last_name="User",
            email="local@example.com",
            role=manager.find_role("Admin"),
            password=os.environ["SUPERSET_ADMIN_PASSWORD"],
        )
        if not user:
            raise RuntimeError("Could not create Superset admin")

    database = db.session.query(Database).filter_by(database_name="Bank ClickHouse").first()
    if not database:
        database = Database(database_name="Bank ClickHouse")
        db.session.add(database)
    database.sqlalchemy_uri = (
        f"clickhousedb://{quote(os.environ['CLICKHOUSE_USER'], safe='')}:"
        f"{quote(os.environ['CLICKHOUSE_PASSWORD'], safe='')}@"
        f"{os.environ['CLICKHOUSE_HOST']}:{os.environ['CLICKHOUSE_PORT']}/{os.environ['CLICKHOUSE_DB']}"
    )
    database.expose_in_sqllab = True
    db.session.flush()
    dataset = (
        db.session.query(SqlaTable)
        .filter_by(table_name="daily_summary", database_id=database.id)
        .first()
    )
    if not dataset:
        dataset = SqlaTable(
            table_name="daily_summary", schema=os.environ["CLICKHOUSE_DB"], database_id=database.id
        )
        db.session.add(dataset)
        db.session.flush()
    dataset.fetch_metadata()
    chart = db.session.query(Slice).filter_by(slice_name="Daily transactions").first()
    if not chart:
        chart = Slice(
            slice_name="Daily transactions",
            viz_type="table",
            datasource_id=dataset.id,
            datasource_type="table",
            params=json.dumps(
                {
                    "query_mode": "raw",
                    "all_columns": [
                        "txn_date",
                        "currency",
                        "transaction_count",
                        "total_amount",
                        "declined_count",
                    ],
                    "row_limit": 1000,
                }
            ),
        )
        db.session.add(chart)
        db.session.flush()
    # Refresh existing chart definitions on upgrades as well as first startup.
    params = json.loads(chart.params or "{}")
    params["all_columns"] = [
        "txn_date",
        "currency",
        "transaction_count",
        "total_amount",
        "declined_count",
        "latest_source_changed_at",
        "latest_loaded_at",
    ]
    chart.params = json.dumps(params)
    dashboard = db.session.query(Dashboard).filter_by(slug="bank-overview").first()
    if not dashboard:
        dashboard = Dashboard(dashboard_title="Bank Overview", slug="bank-overview", published=True)
        db.session.add(dashboard)
    dashboard.slices = [chart]
    dashboard.position_json = json.dumps(
        {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
            "GRID_ID": {
                "id": "GRID_ID",
                "type": "GRID",
                "children": ["ROW-1"],
                "parents": ["ROOT_ID"],
            },
            "ROW-1": {
                "id": "ROW-1",
                "type": "ROW",
                "children": ["CHART-1"],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
            "CHART-1": {
                "id": "CHART-1",
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", "ROW-1"],
                "meta": {
                    "chartId": chart.id,
                    "width": 12,
                    "height": 50,
                    "sliceName": "Daily transactions",
                },
            },
        }
    )
    db.session.commit()
    print("Provisioned Bank Overview dashboard")
