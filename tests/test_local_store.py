from fred_pipeline.audit import RunStatus
from fred_pipeline.config import Environment, PipelineConfig
from fred_pipeline.local_store import LocalWarehouse
from fred_pipeline.manifest import SeriesSpec
from fred_pipeline.pipeline import FredPipeline
from fred_pipeline.transform import daily_feature_matrix


def _config():
    return PipelineConfig(environment=Environment.DEV, fred_api_key="k")


def _spec(series_id, **kw):
    kw.setdefault("title", series_id)
    kw.setdefault("frequency", "d")
    return SeriesSpec(series_id=series_id, **kw)


def test_local_run_persists_all_layers(tmp_path, observations_payload, fake_client_cls):
    db = str(tmp_path / "fred.db")
    client = fake_client_cls({"DGS10": observations_payload})
    wh = LocalWarehouse(_config(), db_path=db)
    pipe = FredPipeline(_config(), client=client, warehouse=wh)

    run = pipe.run([_spec("DGS10")], build_gold_layer=True)
    assert run.status == RunStatus.SUCCEEDED

    # bronze got the verbatim payload
    bronze = wh.query("SELECT * FROM bronze_fred_api_response")
    assert len(bronze) == 1
    assert bronze[0]["observation_count"] == 4

    # silver got 4 normalized rows (3 real values + 1 missing)
    silver = wh.query("SELECT * FROM silver_fred_observation ORDER BY observation_date")
    assert len(silver) == 4
    assert sum(r["is_missing"] for r in silver) == 1

    # gold latest observation built
    latest = wh.query("SELECT * FROM gold_fred_latest_observation")
    assert len(latest) == 4

    # daily feature matrix spans the observation window (4 days, 1 series)
    daily = wh.query("SELECT * FROM gold_fred_macro_feature_daily ORDER BY as_of_date")
    assert len(daily) == 4
    # last day forward-fills the last real value
    assert daily[-1]["value"] == 4.40

    # audit persisted
    assert len(wh.query("SELECT * FROM audit_etl_run")) == 1
    assert len(wh.query("SELECT * FROM audit_etl_series_run")) == 1
    assert len(wh.query("SELECT * FROM audit_data_quality_result")) >= 1
    wh.close()


def test_local_run_is_idempotent(tmp_path, observations_payload, fake_client_cls):
    db = str(tmp_path / "fred.db")
    cfg = _config()

    def one_run():
        wh = LocalWarehouse(cfg, db_path=db)
        pipe = FredPipeline(cfg, client=fake_client_cls({"DGS10": observations_payload}),
                            warehouse=wh)
        pipe.run([_spec("DGS10")])
        n = len(wh.query("SELECT * FROM silver_fred_observation"))
        wh.close()
        return n

    assert one_run() == 4
    # second run MERGEs on the natural key -> still 4, no duplicates
    assert one_run() == 4


def test_meta_sync_registers_full_universe(tmp_path):
    from fred_pipeline.manifest import load_manifests

    db = str(tmp_path / "fred.db")
    wh = LocalWarehouse(_config(), db_path=db)
    counts = wh.sync_meta(load_manifests("manifests"))

    assert counts["fred_series"] == 27
    assert len(wh.query("SELECT * FROM meta_fred_series")) == 27
    assert len(wh.query("SELECT * FROM meta_fred_manifest")) == 4
    # re-sync is idempotent (upsert on primary key)
    wh.sync_meta(load_manifests("manifests"))
    assert len(wh.query("SELECT * FROM meta_fred_series")) == 27
    wh.close()


def test_daily_feature_matrix_forward_fills():
    latest = [
        {"series_id": "X", "observation_date": "2024-01-01", "value": 1.0, "is_missing": False},
        {"series_id": "X", "observation_date": "2024-01-03", "value": 2.0, "is_missing": False},
    ]
    rows = daily_feature_matrix(latest)
    assert [r["as_of_date"] for r in rows] == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert [r["value"] for r in rows] == [1.0, 1.0, 2.0]  # jan-02 forward-filled
    assert rows[1]["raw_value"] is None  # no native release on jan-02
