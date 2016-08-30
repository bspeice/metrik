from pymongo import MongoClient
import logging
import argparse

from metrik import __version__


def open_connection(host, port):
    return MongoClient(host=host, port=port)


def merge(con1, con2, db1, db2):
    database1 = con1[db1]
    database2 = con2[db2]
    collections = database1.collection_names(include_system_collections=False)
    for collection_name in collections:
        collection1 = database1[collection_name]
        collection2 = database2[collection_name]
        for item in collection1.find():
            if '_id' in item and collection2.find_one({'_id': item['_id']}) is None:
                collection2.save(item)
                collection1.delete_one({'_id': item['_id']})
            else:
                logging.warning("Not moving item '{}' as the same ID already"
                                " exists in the `right` database.".format(
                    item.get('_id', '')
                ))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--host-1', dest='host1', required=True,
                        help='The `left` database to copy from')
    parser.add_argument('-f', '--host-2', dest='host2', required=True,
                        help='The `right` database to copy into')
    parser.add_argument('-p', '--port-1', default=27017, dest='port1', type=int,
                        help='The port number of the `left` database')
    parser.add_argument('-o', '--port-2', default=27017, dest='port2', type=int,
                        help='The port number of the `right` database')
    parser.add_argument('-d', '--database-1', default='metrik', dest='db1',
                        help='The database on the `left` host we are merging from')
    parser.add_argument('-s', '--database-2', default='metrik', dest='db2',
                        help='The database on the `right` host we are merging into')
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    con1 = open_connection(args.host1, args.port1)
    con2 = open_connection(args.host2, args.port2)
    merge(con1, con2, args.db1, args.db2)
    con1.close()
    con2.close()


if __name__ == '__main__':
    main()
