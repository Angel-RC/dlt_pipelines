from loguru import logger

def run_pipeline(pipeline, source, resources=None) -> None:
    resources = resources or source().resources.keys()
    if set(resources) == set(source().resources.keys()):
        pipeline.run(source())

    else:
        for resource in resources:
            logger.info(f"Loading data for resource: {resource} ...")
            pipeline.run(source().with_resources(resource))
    