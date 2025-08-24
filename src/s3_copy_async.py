import os
from abc import ABC, abstractmethod
from typing import Optional, List, Tuple, Protocol

import asyncio
import aioboto3
from loguru import logger
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide
from pydantic import Field
from pydantic_settings import BaseSettings


class AWSSettings(BaseSettings):
    aws_access_key_id: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"), env="AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"),
        env="AWS_SECRET_ACCESS_KEY",
    )
    aws_session_token: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SESSION_TOKEN"),
        env="AWS_SESSION_TOKEN",
    )
    aws_region: Optional[str] = Field("ap-northeast-2", env="AWS_DEFAULT_REGION")

    class Config:
        env_file = ".env"


class S3CopySettings(BaseSettings):
    max_concurrent_copies: int = Field(10, env="MAX_CONCURRENT_COPIES")
    multipart_threshold: int = Field(
        1024 * 1024 * 25, env="MULTIPART_THRESHOLD"
    )  # 25MB
    multipart_chunksize: int = Field(
        1024 * 1024 * 25, env="MULTIPART_CHUNKSIZE"
    )  # 25MB
    max_concurrency: int = Field(10, env="MAX_CONCURRENCY")

    class Config:
        env_file = ".env"


class S3ClientProtocol(Protocol):
    async def copy_object(self, **kwargs) -> None: ...
    async def copy(self, copy_source, bucket, key, **kwargs) -> None: ...
    def get_paginator(self, operation_name: str): ...


class IS3SessionFactory(ABC):
    @abstractmethod
    async def create_client(self) -> S3ClientProtocol:
        pass


class IS3Copier(ABC):
    @abstractmethod
    async def copy_single_file(
        self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
    ) -> bool:
        pass

    @abstractmethod
    async def copy_multiple_files(
        self, copy_tasks: List[Tuple[str, str, str, str]]
    ) -> List[bool]:
        pass


class IS3FileListService(ABC):
    @abstractmethod
    async def list_objects_with_prefix(self, bucket: str, prefix: str) -> List[str]:
        pass


class IS3CopyService(ABC):
    @abstractmethod
    async def copy_files_by_prefix(
        self, source_bucket: str, source_prefix: str, dest_bucket: str, dest_prefix: str
    ) -> int:
        pass


class AWSS3SessionFactory(IS3SessionFactory):
    def __init__(self, aws_settings: AWSSettings):
        self.aws_settings = aws_settings
        self.session = aioboto3.Session(
            aws_access_key_id=aws_settings.aws_access_key_id,
            aws_secret_access_key=aws_settings.aws_secret_access_key,
            aws_session_token=aws_settings.aws_session_token,
            region_name=aws_settings.aws_region,
        )

    async def create_client(self) -> S3ClientProtocol:
        return self.session.client("s3")


class S3Copier(IS3Copier):
    def __init__(
        self, session_factory: IS3SessionFactory, copy_settings: S3CopySettings
    ):
        self.session_factory = session_factory
        self.copy_settings = copy_settings
        self.semaphore = asyncio.Semaphore(copy_settings.max_concurrent_copies)

    async def copy_single_file(
        self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
    ) -> bool:
        async with self.semaphore:
            try:
                async with await self.session_factory.create_client() as s3_client:
                    copy_source = {"Bucket": source_bucket, "Key": source_key}

                    await s3_client.copy_object(
                        CopySource=copy_source,
                        Bucket=dest_bucket,
                        Key=dest_key,
                    )

                    return True

            except Exception as e:
                logger.error(
                    f"Failed to copy file {source_bucket}/{source_key} to {dest_bucket}/{dest_key}: {e}"
                )
                return False

    async def copy_multiple_files(
        self, copy_tasks: List[Tuple[str, str, str, str]]
    ) -> List[bool]:
        tasks = [
            self.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
            for source_bucket, source_key, dest_bucket, dest_key in copy_tasks
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed with Exception: {result}")
                processed_results.append(False)
            else:
                processed_results.append(result)

        return processed_results


class S3FileListService(IS3FileListService):
    def __init__(self, session_factory: IS3SessionFactory):
        self.session_factory = session_factory

    async def list_objects_with_prefix(self, bucket: str, prefix: str) -> List[str]:
        async with await self.session_factory.create_client() as s3_client:
            objects = []
            paginator = s3_client.get_paginator("list_objects_v2")

            async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in page:
                    objects.extend([obj["Key"] for obj in page["Contents"]])

            return objects


class S3CopyService(IS3CopyService):
    def __init__(self, copier: IS3Copier, file_list_service: IS3FileListService):
        self.copier = copier
        self.file_list_service = file_list_service

    async def copy_files_by_prefix(
        self,
        source_bucket: str,
        source_prefix: str,
        dest_bucket: str,
        dest_prefix: Optional[str] = None,
    ) -> int:
        if dest_prefix is None:
            dest_prefix = source_prefix

        source_keys = await self.file_list_service.list_objects_with_prefix(
            source_bucket, source_prefix
        )
        if not source_keys:
            logger.info(f"No objects found with prefix {source_prefix}")
            return 0

        copy_tasks = []
        for source_key in source_keys:
            dest_key = source_key.replace(source_prefix, dest_prefix, 1)
            copy_tasks.append((source_bucket, source_key, dest_bucket, dest_key))

        logger.info(f"Found {len(copy_tasks)} files to copy")

        results = await self.copier.copy_multiple_files(copy_tasks)
        successful_copies = sum(1 for result in results if result)

        logger.info(
            f"Successfully copied {successful_copies} files out of {len(copy_tasks)}"
        )

        return successful_copies


class S3LargeFileCopier(IS3Copier):
    def __init__(
        self, session_factory: IS3SessionFactory, copy_settings: S3CopySettings
    ):
        self.session_factory = session_factory
        self.copy_settings = copy_settings
        self.semaphore = asyncio.Semaphore(copy_settings.max_concurrency)

    async def copy_single_file(
        self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
    ) -> bool:
        async with self.semaphore:
            try:
                async with await self.session_factory.create_client() as s3_client:
                    copy_source = {"Bucket": source_bucket, "Key": source_key}

                    config = aioboto3.s3.transfer.TransferConfig(
                        multipart_threshold=self.copy_settings.multipart_threshold,
                        max_concurrency=self.copy_settings.max_concurrency,
                        multipart_chunksize=self.copy_settings.multipart_chunksize,
                        use_threads=True,
                    )

                    await s3_client.copy(
                        copy_source, dest_bucket, dest_key, Config=config
                    )

                    return True

            except Exception as e:
                logger.error(
                    f"Failed to copy file {source_bucket}/{source_key} to {dest_bucket}/{dest_key}: {e}"
                )

                return False

    async def copy_multiple_files(
        self, copy_tasks: List[Tuple[str, str, str, str]]
    ) -> List[bool]:
        tasks = [
            self.copy_single_file(source_bucket, source_key, dest_bucket, dest_key)
            for source_bucket, source_key, dest_bucket, dest_key in copy_tasks
        ]

        return await asyncio.gather(*tasks, return_exceptions=True)


class ApplicationContainer(containers.DeclarativeContainer):
    aws_settings = providers.Singleton(AWSSettings)
    copy_settings = providers.Singleton(S3CopySettings)

    session_factory = providers.Singleton(
        AWSS3SessionFactory, aws_settings=aws_settings
    )

    s3_copier = providers.Singleton(
        S3Copier, session_factory=session_factory, copy_settings=copy_settings
    )
    s3_large_file_copier = providers.Singleton(
        S3LargeFileCopier, session_factory=session_factory, copy_settings=copy_settings
    )
    s3_file_list_service = providers.Factory(
        S3FileListService, session_factory=session_factory
    )
    s3_copy_service = providers.Factory(
        S3CopyService, copier=s3_copier, file_list_service=s3_file_list_service
    )


class S3CopyApplication:
    @inject
    def __init__(
        self,
        copy_service: IS3CopyService = Provide[ApplicationContainer.s3_copy_service],
        copier: IS3Copier = Provide[ApplicationContainer.s3_copier],
        large_file_copier: IS3Copier = Provide[
            ApplicationContainer.s3_large_file_copier
        ],
    ):
        self.copy_service = copy_service
        self.copier = copier
        self.large_file_copier = large_file_copier

    async def copy_single_file(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
        use_large_file_copier: bool = False,
    ) -> bool:
        copier = self.large_file_copier if use_large_file_copier else self.copier
        return await copier.copy_single_file(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
        )

    async def copy_multiple_files(
        self,
        copy_tasks: List[Tuple[str, str, str, str]],
        use_large_file_copier: bool = False,
    ) -> List[bool]:
        copier = self.large_file_copier if use_large_file_copier else self.copier
        return await copier.copy_multiple_files(copy_tasks)

    async def copy_by_prefix(
        self,
        source_bucket: str,
        source_prefix: str,
        dest_bucket: str,
        dest_prefix: Optional[str] = None,
    ) -> int:
        return await self.copy_service.copy_files_by_prefix(
            source_bucket=source_bucket,
            source_prefix=source_prefix,
            dest_bucket=dest_bucket,
            dest_prefix=dest_prefix,
        )
