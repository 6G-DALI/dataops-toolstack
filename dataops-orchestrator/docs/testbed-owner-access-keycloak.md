# Testbed owner access: Keycloak setup log

Records what was added so that the owners of a testbed can use the DataOps testbed pages for their own testbed only, and the Keycloak configuration it needs. Written 2026-10-09.

## 1. Decision

- One Keycloak **group per testbed**: `/testbeds/<slug>`. Members own that testbed.
- The existing realm role **`testbed-admin`** keeps managing every testbed and the registry, as before.
- The orchestrator enforces access. The UI only hides what a user cannot use.

| | `testbed-admin` | Member of `/testbeds/<slug>` |
|---|---|---|
| List testbeds | all | only their own |
| Detail, assets, audit log, connector bundle | all | their own |
| Find asset, find transfer, negotiate, start transfer | all | their own |
| Register, provision, rotate Data Lake key, deregister | yes | no (403) |
| List, add (by e-mail) and remove the testbed's members | yes | no (403) |

A user in no testbed group gets an empty list and 403 on every `/testbeds/<slug>` route.

## 2. What changed in the code

Repository `dataops` (`main`), commit `0c82b51`:

- `dataops-orchestrator/auth.py`: `current_claims` validates the token. `require_testbed_admin` (role), `require_testbed_user` (any signed-in user) and `require_testbed_access(slug)` (admin or member of that testbed's group) build on it. Group values are accepted as paths (`/testbeds/kul`) or bare names (`kul`).
- `dataops-orchestrator/routers/testbeds.py`: the router-wide admin dependency is replaced by per-route dependencies (table above). The list is filtered by the caller's groups. Provisioning gets an `access` step.
- `dataops-orchestrator/keycloak_admin.py` (new): creates `/testbeds` (if missing) and `/testbeds/<slug>` through the Keycloak admin API, using a service-account client (client-credentials grant). An existing group counts as success.
- `dataops-orchestrator/config.py`: `TESTBED_GROUP_PREFIX` (default `testbeds`), `KEYCLOAK_ADMIN_CLIENT_ID`, `KEYCLOAK_ADMIN_CLIENT_SECRET`.
- Members (later change): `GET/POST /testbeds/<slug>/members` and `DELETE /testbeds/<slug>/members/<user_id>`, admin-only, through the Keycloak admin API; the UI has a **Members** card on the testbed page for admins. Adding looks the user up by exact e-mail address (404 if none, 409 if several) and creates the group if it is missing. Each add and remove is written to the audit log.
- `dataops-ui/src/auth/testbedAccess.ts` (new) and `Layout`, `TestbedList`, `TestbedDetail`: the Testbeds menu shows for admins and for members of any `/testbeds/...` group. Register, Re-run provisioning, Rotate key and Deregister show for admins only. The Provisioning card lists the `access` step.
- Tests: 4 for the group rules, 3 for group creation and the `access` step. After merging with upstream, 53 orchestrator tests pass and the UI type-checks.

Repository `dataops-devops` (`master`), commit `7b85c31`: `docker/prd/dataops/docker-compose.yaml` and `.env.example` pass `TESTBED_GROUP_PREFIX`, `KEYCLOAK_ADMIN_CLIENT_ID` and `KEYCLOAK_ADMIN_CLIENT_SECRET` to the orchestrator.

The `access` step: with the admin client configured, provisioning creates the group; failure marks the step failed. Without it the step is shown as **skipped** and does not block the testbed from becoming `provisioned`; the group is then created by hand.

## 3. Keycloak setup (realm `dspace`)

### A. Put the groups into the token

Clients → `dataops-ui` → Client scopes → `dataops-ui-dedicated` → Add mapper → By configuration → **Group Membership**.

| Field | Value |
|---|---|
| Name | `groups` |
| Token Claim Name | `groups` |
| Full group path | On (gives `/testbeds/kul`; Off gives `kul`, also accepted) |
| Add to access token | **On** (required: the orchestrator validates the access token) |
| Add to ID token | On (optional, helps debugging) |
| Add to lightweight access token | Off |
| Add to userinfo | Off |
| Add to token introspection | On if shown (not used) |

Also check that **Full scope allowed** is on for `dataops-ui` (dedicated scope → Scope), otherwise claims can be filtered. Client scopes → Evaluate shows the resulting claims for a test user.

### B. Let the orchestrator create groups (optional)

1. Clients → Create client: Client ID `dataops-orchestrator`, Client authentication **On**, Authentication flow: only **Service accounts roles**.
2. Credentials tab: copy the Client secret.
3. Service accounts roles tab → Assign role → filter by clients → `realm-management`: **manage-users**, **query-groups** and **view-users** (view-users is needed to find a user by e-mail and to list a group's members).
4. On the server, in `.env.prd` (dataops stack):
   ```
   KEYCLOAK_ADMIN_CLIENT_ID=dataops-orchestrator
   KEYCLOAK_ADMIN_CLIENT_SECRET=<secret>
   ```
   then `docker compose --env-file .env.prd up -d dataops-orchestrator`.

Without B, do D by hand.

### C. Existing testbeds

With B: open each testbed in the DataOps UI and click **Re-run provisioning**. The `access` step creates `/testbeds` once and then `/testbeds/<slug>`. Without B, do D.

### D. Create a group by hand

Groups → Create group `testbeds`; open it → Create child group named exactly the testbed slug.

### E. Add owners

In the DataOps UI (needs B): open the testbed → **Members** card → type the user's e-mail address → Add. Admins can also remove members there. The user must already exist in Keycloak (they have signed in to a DALI application once); otherwise the UI says so.

In Keycloak: Groups → `testbeds` → `<slug>` → Members → Add member. Owners do not need `testbed-admin`. They must log out and in again, because the groups are added to the token at login.

### F. Check

- An owner sees the Testbeds menu with only their testbed, and no Register, Provision, Rotate or Deregister buttons.
- In the browser, the decoded access token contains `groups: ["/testbeds/<slug>"]`.

### G. DataOps operators group (UI pages)

The DataOps pages (DAGs, Tasks, Datasets, Add Dataset, Services and everything under them) are shown only to members of the Keycloak group **`dataops-operators`**. The Home page is open to everyone signed in and gates its own blocks: the DAG and run dashboard is for operators, and a Testbeds block (the testbeds the user can see: all of them for a testbed admin, their own for an owner) is for anyone with testbed access. Someone with neither sees a "No access yet" note.

1. Groups → Create group `dataops-operators` (top level). Add the people who run DataOps. The `groups` mapper from step A already delivers it, as `/dataops-operators`.
2. Everyone else (testbed owners, testbed admins who are not operators) sees the Home page, with only the Testbeds block, and the Testbeds menu; the other pages give a "No access" message.
3. **Existing users lose the DataOps pages until they are added to the group.** Add them before deploying the UI. Users must log out and in again.

This hides the pages in the UI only. The orchestrator endpoints behind them (and Airflow through it) are still not authenticated, so it is not an access control on its own.

## 4. Deploy

1. Redeploy the orchestrator and the UI (the dataops stack workflow, from `main`).
2. Apply the `dataops-devops` compose change on the server and set the variables from B if used.
3. Do the Keycloak steps A to E.

## 5. Troubleshooting

| Symptom | Likely cause |
|---|---|
| Owner sees an empty list or 403 | Mapper from A missing or Add to access token off, user not in the group, group named differently from the slug, or the user has not logged in again |
| Owner has no Testbeds menu | The token has no `groups` claim, or the group is not under `/testbeds` (see `TESTBED_GROUP_PREFIX`) |
| `access` step failed: `403`, or the Members card shows a 403 | The service account lacks `manage-users`, `query-groups` or `view-users` |
| Members card: "No Keycloak user with the e-mail ..." | The person has not signed in to a DALI application yet, or the address differs from the one on their Keycloak account |
| Members card: "KEYCLOAK_ADMIN_CLIENT_ID/_SECRET not set" | Step B is not done |
| `access` step failed: `401` or invalid client | Wrong `KEYCLOAK_ADMIN_CLIENT_ID` or secret, or Service accounts roles not enabled on the client |
| `access` step skipped | `KEYCLOAK_ADMIN_CLIENT_ID` / `_SECRET` not set in the orchestrator's environment |
| Group URL wrong in an older Keycloak | Admin API path is derived from `KEYCLOAK_ISSUER` (`/realms/` becomes `/admin/realms/`); check the issuer includes `/auth` if your Keycloak serves under it |

## 6. Status and open points

- Tested with unit tests and a stand-in for the Keycloak admin API. **Not yet run against the real Keycloak** or deployed: steps A to E above are still to be done there.
- Deregistering a testbed does not delete its Keycloak group (the optional bucket and catalogue deletion does not cover it either), so remove it by hand if the testbed is gone for good.
- The rest of the orchestrator (Airflow proxy and similar endpoints) is still unauthenticated; owners can reach those URLs directly if they know them.
