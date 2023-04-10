from base.checker import(
    CreateCollectionChecker,
    DropCollectionChecker,
    CreatePartitionChecker,
    DropPartitionChecker,
    InsertEntitiesCollectionChecker,
    InsertEntitiesPartitionChecker,
    DeleteEntitiesCollectionChecker,
    DeleteEntitiesPartitionChecker
)
import time

class TestDemo:

    def test_create_collection(self):
        checker = CreateCollectionChecker("10.101.211.232", 19530)
        checker.run()
        time.sleep(2)
        print(checker.pause)
        print(type(checker.pause))
        checker.pause()
        time.sleep(3)
        checker.resume()
        time.sleep(3)
    
    def test_drop_collection(self):
        checker = CreateCollectionChecker("10.101.211.232", 19530, c_name_prefix="cdc_test_drop")
        checker.run()
        time.sleep(10)
        checker.terminate()
        checker = DropCollectionChecker("10.101.211.232", 19530, c_name_prefix="cdc_test_drop")
        checker.run()
        time.sleep(10)
        checker.terminate()
        print(checker.deleted_collections)

    def test_create_partition(self):
        checker = CreatePartitionChecker("10.101.211.232", 19530, c_name="cdc_test_partition")
        checker.run()
        time.sleep(2)
        print(checker.pause)
        print(type(checker.pause))
        checker.pause()
        time.sleep(3)
        checker.resume()
        time.sleep(3)
        assert 1 == 1

    def test_drop_partition(self):
        checker = CreatePartitionChecker("10.101.211.232", 19530, c_name="cdc_create_partition_test")
        checker.run()
        time.sleep(2)
        print(checker.pause)
        print(type(checker.pause))
        checker.pause()
        time.sleep(3)
        checker.resume()
        time.sleep(3)
        checker.terminate()
        checker = DropPartitionChecker("10.101.211.232", 19530, c_name="cdc_create_partition_test")
        checker.run()
        time.sleep(6)
        checker.pause()
        time.sleep(3)
        checker.resume()
        time.sleep(3)

    def test_insert_collection(self):
        checker = InsertEntitiesCollectionChecker("10.101.211.232", 19530, c_name="cdc_insert_collection_test")
        checker.run()
        time.sleep(40)
        print(checker.pause)
        print(type(checker.pause))
        checker.pause()
        time.sleep(3)
        checker.resume()
        time.sleep(3)
    
    def test_insert_partition(self):
        checker = InsertEntitiesPartitionChecker("10.101.211.232", 19530, c_name="cdc_insert_collection_test")
        checker.run()
        time.sleep(120)
        print(checker.pause)
        print(type(checker.pause))
        checker.pause()
        time.sleep(60)
        checker.resume()
        time.sleep(60)

    def test_delete_entities_collection(self):
        checker = InsertEntitiesCollectionChecker("10.101.211.232", 19530, c_name="cdc_insert_collection_test")
        checker.run()
        time.sleep(120)
        checker.terminate()
        checker = DeleteEntitiesCollectionChecker("10.101.211.232", 19530, c_name="cdc_insert_collection_test")
        checker.run()
        time.sleep(120)
        checker.pause()
        time.sleep(60)
        checker.resume()
        time.sleep(60)

    def test_delete_entities_collection(self):
        checker = InsertEntitiesPartitionChecker("10.101.211.232", 19530, c_name="cdc_test", p_name="p1")
        checker.run()
        time.sleep(120)
        checker.terminate()
        checker = DeleteEntitiesPartitionChecker("10.101.211.232", 19530, c_name="cdc_test", p_name="p1")
        checker.run()
        time.sleep(120)
        checker.pause()
        time.sleep(60)
        checker.resume()
        time.sleep(60)