#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "table/block.h"
#include "table/block_builder.h"

namespace leveldb {
    /**
   BlockBuilder {
    ...
    private:
        const Options* options_;
        std::string buffer_;              // Destination buffer
        std::vector<uint32_t> restarts_;  // Restart points
        int counter_;                     // Number of entries emitted since restart
        bool finished_;                   // Has Finish() been called?
        std::string last_key_;
   }
   */
    class BlockBuilderAccessor {
        public:
            static std::string GetBuffer(BlockBuilder* blockBuilder){
                void* blockBuilderPtr = (void *)blockBuilder;
                void* bufferPtr = reinterpret_cast<char*>(blockBuilderPtr) + sizeof(const Options*);
                return *(reinterpret_cast<std::string*>(bufferPtr));
            }

            static std::vector<uint32_t> GetRestarts(BlockBuilder* blockBuilder){
                void* blockBuilderPtr = (void *)blockBuilder;
                void* restartsPtr = reinterpret_cast<char*>(blockBuilderPtr) + sizeof(const Options*) + sizeof(std::string);
                return *(reinterpret_cast<std::vector<uint32_t>*>(restartsPtr));
            }

            static int GetCounter(BlockBuilder* blockBuilder){
                void* blockBuilderPtr = (void *)blockBuilder;
                void* counterPtr = reinterpret_cast<char*>(blockBuilderPtr) + sizeof(const Options*) + sizeof(std::string) + sizeof(std::vector<uint32_t>);
                return *(reinterpret_cast<int*>(counterPtr));
            }

            static bool GetFinished(BlockBuilder* blockBuilder){
                void* blockBuilderPtr = (void *)blockBuilder;
                void* finishedPtr = reinterpret_cast<char*>(blockBuilderPtr) + sizeof(const Options*) + sizeof(std::string) + sizeof(std::vector<uint32_t>) + sizeof(int);
                return *(reinterpret_cast<bool*>(finishedPtr));
            }
            
            static std::string GetLastKey(BlockBuilder* blockBuilder){
                void* blockBuilderPtr = (void *)blockBuilder;
                void* lastKeyPtr = reinterpret_cast<char*>(blockBuilderPtr) + sizeof(const Options*) + sizeof(std::string) + sizeof(std::vector<uint32_t>) + sizeof(int) + sizeof(int);
                return *(reinterpret_cast<std::string*>(lastKeyPtr));
            }
    };
    
    TEST(BlockBuilderTest, KEY_PREFIX){
        Options options;
        options.block_restart_interval = 2;
        BlockBuilder blockBuilder(&options);
        auto restarts = BlockBuilderAccessor::GetRestarts(&blockBuilder);
        // default restarts size is 1, the first restart point is at offset 0
        ASSERT_EQ(restarts.size(), 1);
        ASSERT_EQ(restarts[0], 0);

        blockBuilder.Add("foo", "v1");
        auto buffer = BlockBuilderAccessor::GetBuffer(&blockBuilder);
        // shared key prefix bytes: 0
        // unshared key bytes: 3
        // value length: 2
        // unsahred key bytes: foo
        // value: v1
        ASSERT_EQ(buffer, std::string("\000\003\002foov1", 8));
        auto lastKey = BlockBuilderAccessor::GetLastKey(&blockBuilder);
        ASSERT_EQ(lastKey, "foo");

        blockBuilder.Add("fox", "v2");
        buffer = BlockBuilderAccessor::GetBuffer(&blockBuilder);
        // shared key prefix bytes: 2
        // unshared key bytes: 1
        // value length: 2
        // unshared key bytes: x
        // value: v2
        ASSERT_EQ(buffer, std::string("\000\003\002foov1\002\001\002xv2", 8 + 6));
        lastKey = BlockBuilderAccessor::GetLastKey(&blockBuilder);
        ASSERT_EQ(lastKey, "fox");

        auto counter = BlockBuilderAccessor::GetCounter(&blockBuilder);
        ASSERT_EQ(counter, 2);

        blockBuilder.Add("foy", "v3");
        // block_restart_interval == 2, so restart here. counter_ reset, restarts record the offset of the restart point, prefix bytes reset
        counter = BlockBuilderAccessor::GetCounter(&blockBuilder);
        ASSERT_EQ(counter, 1);
        restarts = BlockBuilderAccessor::GetRestarts(&blockBuilder);
        ASSERT_EQ(restarts.size(), 2);
        // the last restart point is at offset 14 of the buffer
        ASSERT_EQ(restarts[1], 14);
        // shared key prefix bytes: 0
        // unshared key bytes: 3
        // value length: 2
        // unsahred key bytes: foy
        // value: v3
        buffer = BlockBuilderAccessor::GetBuffer(&blockBuilder);
        ASSERT_EQ(buffer, std::string("\000\003\002foov1\002\001\002xv2\000\003\002foyv3", 14 + 8));
        lastKey = BlockBuilderAccessor::GetLastKey(&blockBuilder);
        ASSERT_EQ(lastKey, "foy");

        blockBuilder.Finish();        
        auto finished = BlockBuilderAccessor::GetFinished(&blockBuilder);
        ASSERT_EQ(finished, true);
        // the restart array is appended to the buffer
        //  the _buffer before append restart array
        //  --> append restart array[0], by PutFixed32, \000\000\000\000
        //  --> append restart array[1], by PutFixed32, \x0e\000\000\000
        //  --> append restart array size, by PutFixed32, \x02\000\000\000
        buffer = BlockBuilderAccessor::GetBuffer(&blockBuilder);
        ASSERT_EQ(buffer, std::string("\000\003\002foov1\002\001\002xv2\000\003\002foyv3\000\000\000\000\x0e\000\000\000\x02\000\000\000", 22 + 12)); 
    }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
