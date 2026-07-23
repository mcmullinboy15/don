//! Docker image building — tar context creation and streamed build output.

use bollard::Docker;
use bollard::query_parameters::BuildImageOptionsBuilder;
use bytes::Bytes;
use futures_util::StreamExt;
use std::path::Path;

use crate::config::service::DockerBuildConfig;
use crate::output::ServiceWriter;

use super::DockerError;

/// Build a Docker image from a `DockerBuildConfig`.
///
/// 1. Creates a tar archive of the build context directory.
/// 2. Streams the archive to the Docker daemon.
/// 3. Streams build output through the service writer for display.
/// 4. Returns an error if the build fails.
pub(crate) async fn build_image(
    client: &Docker,
    config: &DockerBuildConfig,
    image_tag: &str,
    base_dir: &Path,
    writer: &ServiceWriter,
) -> Result<(), DockerError> {
    let context_path = base_dir.join(&config.context);

    // Create tar archive of the build context.
    let tar_body = create_tar_context(&context_path)?;

    let mut options_builder = BuildImageOptionsBuilder::new()
        .t(image_tag)
        .dockerfile(config.dockerfile.as_deref().unwrap_or("Dockerfile"))
        .rm(true);

    if let Some(ref target) = config.target {
        options_builder = options_builder.target(target.as_str());
    }

    if !config.args.is_empty() {
        options_builder = options_builder.buildargs(&config.args);
    }

    let options = options_builder.build();
    let body = bollard::body_stream(futures_util::stream::once(
        async move { Bytes::from(tar_body) },
    ));

    let mut stream = client.build_image(options, None, Some(body));
    while let Some(result) = stream.next().await {
        match result {
            Ok(info) => {
                if let Some(ref error_detail) = info.error_detail {
                    let msg = error_detail
                        .message
                        .as_deref()
                        .unwrap_or("unknown build error");
                    return Err(DockerError::BuildFailed(msg.to_string()));
                }
                if let Some(ref line) = info.stream {
                    let trimmed = line.trim_end();
                    if !trimmed.is_empty() {
                        writer.write_line(trimmed).await;
                    }
                }
            }
            Err(e) => return Err(DockerError::Api(e)),
        }
    }

    Ok(())
}

/// Create a tar archive of a directory for Docker build context.
///
/// Skips don's own runtime directory (`.don`): when the build context is the
/// project root, it holds a live control socket that cannot be archived (and has
/// no business in an image). Everything else is archived as-is.
fn create_tar_context(context_path: &Path) -> Result<Vec<u8>, DockerError> {
    let mut tar_builder = tar::Builder::new(Vec::new());
    for entry in std::fs::read_dir(context_path).map_err(DockerError::Tar)? {
        let entry = entry.map_err(DockerError::Tar)?;
        let name = entry.file_name();
        if name == ".don" {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(DockerError::Tar)?;
        if file_type.is_dir() {
            tar_builder
                .append_dir_all(&name, &path)
                .map_err(DockerError::Tar)?;
        } else {
            tar_builder
                .append_path_with_name(&path, &name)
                .map_err(DockerError::Tar)?;
        }
    }
    tar_builder.into_inner().map_err(DockerError::Tar)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_create_tar_context() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Dockerfile"), "FROM alpine\n").unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("src/main.rs"), "fn main() {}").unwrap();

        let tar_bytes = create_tar_context(dir.path()).unwrap();
        assert!(!tar_bytes.is_empty());

        // Verify the tar contains the expected files.
        let mut archive = tar::Archive::new(tar_bytes.as_slice());
        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().unwrap().to_string_lossy().into_owned())
            .collect();

        assert!(
            entries.iter().any(|e| e.contains("Dockerfile")),
            "expected Dockerfile in tar: {entries:?}"
        );
        assert!(
            entries.iter().any(|e| e.contains("src/main.rs")),
            "expected src/main.rs in tar: {entries:?}"
        );
    }

    #[test]
    fn test_tar_context_excludes_don_dir() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Dockerfile"), "FROM alpine\n").unwrap();
        // don's runtime dir — in a real run this also holds a socket that can't
        // be archived. It must not appear in the build context.
        std::fs::create_dir_all(dir.path().join(".don")).unwrap();
        std::fs::write(dir.path().join(".don/state.json"), "{}").unwrap();

        let tar_bytes = create_tar_context(dir.path()).unwrap();
        let mut archive = tar::Archive::new(tar_bytes.as_slice());
        let entries: Vec<String> = archive
            .entries()
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().unwrap().to_string_lossy().into_owned())
            .collect();

        assert!(
            entries.iter().any(|e| e.contains("Dockerfile")),
            "expected Dockerfile in tar: {entries:?}"
        );
        assert!(
            !entries.iter().any(|e| e.contains(".don")),
            "expected .don to be excluded from tar: {entries:?}"
        );
    }
}
