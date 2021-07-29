import argparse
import importlib
 
from src.common.builder import Builder
from src.common.logger import get_logger
from src.common.run_config import RunConfigurations

log = get_logger(__name__)


def run_main():
    try:
        config = RunConfigurations(args.config_path, args.run_mode, args.config_bucket)
        log.info('Builder Name -- %s' % config.builder_name)
        log.info('Builder Location -- %s' % config.builder_module)

        module = importlib.import_module(config.builder_module)
        builder = getattr(module, config.builder_name)
        builder_obj = builder(config)
        if isinstance(builder_obj, Builder):
            builder_obj.build()
        else:
            raise  (" Builder should be src.common.builder.Builder Type")

    except Exception as e:
        log.exception(e)
        raise  (f'''Exception in {config.builder_name} and the error is {e}''')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spark Application')
    parser.add_argument('--run_mode', help='Where the application Runs (Local/Cluster)', default='cluster')
    parser.add_argument('--config_bucket', help='Config Bucket Name', required=True)
    parser.add_argument('--config_path', help='The Location of config file', required=True)
    args, unknown = parser.parse_known_args()
    run_main()
