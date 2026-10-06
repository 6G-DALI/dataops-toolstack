"""
Registers the testbeds that were onboarded by hand (ISI, KUL, EUR) in the
registry, so they show up in the UI next to newly registered ones.

Run once inside the orchestrator container (it reads DATABASE_URL and
TESTBED_SECRET_KEY from the environment, no admin token needed):

    docker compose --env-file .env.prd exec dataops-orchestrator python seed_testbeds.py

Safe to repeat: a testbed whose slug is already registered is left untouched.

They are stored as status "adopted": their bucket, catalogue and transfer
already exist and keep working as they are. No Data Lake key is created here,
so existing transfers are not affected. Open the testbed in the UI and run
"Re-run provisioning" when you want it to get its own bucket-scoped key (a
transfer keeps using its old key until it is restarted).

Values come from docker/prd/connector-<testbed>/*.properties and
docker/prd/scripts/1-prepare-contract_<testbed>_*.py in dataops-devops.
"""

import testbed_store as store

EXISTING = [
    {
        "slug": "isi", "name": "ISI", "organisation": "Athena RC",
        "participant_id": "provider-isi", "ids_id": "urn:connector:provider-isi",
        "experiment_prefix": "6g-dali-staging-isi", "bucket": "6g-dali-staging-isi",
        # From the contract script. The callback address in isi_connector.properties
        # (http://6gdali-facility-edc:20002/protocol) looks like a leftover from the
        # central connector: check it against the real ISI host.
        "dsp_url": "http://6gdali-isi-edc:18192/protocol",
    },
    {
        "slug": "kul", "name": "KUL", "organisation": "KU Leuven",
        "participant_id": "provider-kul", "ids_id": "urn:connector:provider-kul",
        "experiment_prefix": "6g-dali-staging-kul", "bucket": "6g-dali-staging-kul",
        "dsp_url": "http://6gdali-kul-edc:18192/protocol",
    },
    {
        "slug": "eur", "name": "EUR", "organisation": None,
        # EUR's participant id really is "provider" (not provider-eur).
        "participant_id": "provider", "ids_id": "urn:connector:provider",
        "experiment_prefix": "6g-dali-staging-eur", "bucket": "6g-dali-staging-eur",
        "dsp_url": "http://eur.testbeds.6gdali.sparkworks.net:18192/protocol",
    },
]


def seed(actor: str = "seed_testbeds.py") -> list[str]:
    added = []
    for tb in EXISTING:
        if store.get_testbed(tb["slug"]):
            print(f"{tb['slug']}: already registered, skipped")
            continue
        values = {**tb, "catalogue_id": tb["bucket"], "contact_email": None, "produced_by_iri": None}
        store.create_testbed(values, actor)
        store.update_testbed(tb["slug"], status="adopted")
        store.audit(tb["slug"], actor, "adopt", "existing testbed registered from its connector config")
        print(f"{tb['slug']}: registered as adopted")
        added.append(tb["slug"])
    return added


if __name__ == "__main__":
    seed()
