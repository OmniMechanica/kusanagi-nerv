"""Load a registered inference artifact only after verifying its bytes."""

import io
import pickle
import struct


class InvalidArtifact(ValueError):
    """The registered artifact is absent, corrupt or incompatible."""


class ArtifactUnavailable(OSError):
    """The artifact cannot be verified right now."""


def load_artifact(uri: str, sha256: str):
    try:
        import torch

        from kusanagi_nerv.training.loop import load_model
        from kusanagi_nerv.training.object_store import (
            ObjectNotFound,
            ObjectStore,
            ObjectStoreSettings,
            StorageUnavailable,
        )

        store = ObjectStore(ObjectStoreSettings.from_environment())
    except (ModuleNotFoundError, ValueError) as exc:
        raise ArtifactUnavailable("Artifact verifier is unavailable") from exc
    try:
        try:
            data = store.read(uri, sha256)
        except (ObjectNotFound, ValueError) as exc:
            raise InvalidArtifact("Invalid or missing model artifact") from exc
        except StorageUnavailable as exc:
            raise ArtifactUnavailable("Artifact storage is unavailable") from exc
    finally:
        store.client.close()
    try:
        model = load_model(io.BytesIO(data))
    except (
        ValueError,
        RuntimeError,
        EOFError,
        pickle.UnpicklingError,
        struct.error,
        KeyError,
        TypeError,
        IndexError,
    ) as exc:
        raise InvalidArtifact("Unsupported model artifact") from exc
    if any(
        not torch.isfinite(parameter).all().item() for parameter in model.parameters()
    ):
        raise InvalidArtifact("Model parameters must be finite")
    return model
