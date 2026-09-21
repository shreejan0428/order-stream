import argparse
import json
import logging
import time

from orderstream import broker, storage


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser(description="Run the order event pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    lag = commands.add_parser("lag")
    lag.add_argument("--group", default="orders-live")
    relay = commands.add_parser("relay")
    relay.add_argument("--once", action="store_true")
    worker = commands.add_parser("consume")
    worker.add_argument("--group", default="orders-live")
    worker.add_argument("--projection", default="live")
    worker.add_argument("--max-messages", type=int, default=0)
    worker.add_argument("--timeout", type=int, default=0)
    worker.add_argument("--crash-after-apply", action="store_true")
    totals = commands.add_parser("summary")
    totals.add_argument("--projection", default="live")
    args = parser.parse_args()
    if args.command == "init":
        storage.initialize()
        broker.initialize_topic()
    elif args.command == "relay":
        client = broker.producer()
        while True:
            count = broker.publish_pending(client)
            logging.info("published=%s", count)
            if args.once:
                break
            time.sleep(1)
    elif args.command == "lag":
        print(json.dumps(broker.consumer_lag(args.group), indent=2))
    elif args.command == "consume":
        broker.consume(
            args.group,
            args.projection,
            args.max_messages,
            args.timeout,
            args.crash_after_apply,
        )
    else:
        print(json.dumps(storage.summary(args.projection), indent=2, default=str))


if __name__ == "__main__":
    main()
