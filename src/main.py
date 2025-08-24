import asyncio

from s3_copy_async import ApplicationContainer, S3CopyApplication


async def main():
    container = ApplicationContainer()
    container.wire(modules=[__name__])
    
    app = S3CopyApplication()
    
    try:
        success = await app.copy_single_file(
            source_bucket="",
            source_key="",
            dest_bucket="",
            dest_key=""
        )
        print(f"Copy successful: {success}")
    except Exception as e:
        print(f"Error: {e}")
        
if __name__ == "__main__":
    asyncio.run(main())