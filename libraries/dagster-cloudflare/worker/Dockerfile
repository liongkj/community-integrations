# The run image is your Dagster code image. The launcher sets the entrypoint to
# the `dagster api execute_run` command, so the image needs nothing extra.
# CODE_IMAGE comes from build_vars in wrangler.jsonc. Use the same digest-pinned
# reference as the code server's DAGSTER_CURRENT_IMAGE.
# Cloudflare Containers run linux/amd64 images.
ARG CODE_IMAGE
FROM --platform=linux/amd64 ${CODE_IMAGE}
