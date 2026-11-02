# DataIngestor

Pairs Wowza HLS frames with Kafka telemetry per tail and archives each flight to MinIO. The message format
it reads is described in [docs/ICD.md](docs/ICD.md).

## Deployment

A push to `master` deploys itself:

1. **Build.** The Gitea runner on the marcopolo host ([.gitea/workflows/build.yml](.gitea/workflows/build.yml))
   builds `dataingestor:latest` straight into that host's Docker, also tagged with the commit.
2. **Redeploy.** When the build run finishes, Gitea's `portainer-deploy` webhook (event: Workflow Run) calls
   the Portainer stack's webhook. Portainer runs [docker-compose.yml](docker-compose.yml), and the container is
   recreated if the image changed.

The image is built by the runner, not by Portainer, because Portainer can't build images through its agent.

### Stack settings in Portainer

| Setting | Value |
|---|---|
| Name | `dataingestor` (keeps the `dataingestor_output` volume, where flights wait to be uploaded) |
| Repository reference | `refs/heads/master` |
| Compose path | `docker-compose.yml` |
| Environment | `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY` (MinIO); optionally `HOST_IP` (default `10.10.40.20`) |

Kafka, Wowza and MinIO are reached at `HOST_IP`, so `appsettings.json` keeps no machine-specific addresses.

### Rolling back

Each build is tagged with its commit. To run an earlier one, retag it and redeploy the stack in Portainer:

```bash
docker tag dataingestor:<commit> dataingestor:latest
```
