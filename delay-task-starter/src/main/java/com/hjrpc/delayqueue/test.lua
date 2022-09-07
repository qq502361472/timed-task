--// 1. 推送到ZSET taskId/taskData/executeTime/retryTimes(使用struct.pack()打包)
--// 2. 推送到hash KEY：taskData VAL: taskId
-- key = classNameWithPrefix
-- val = taskId/taskDataString/executeTime/retryTimes

-- offer
local score = tonumber(ARGV[3]);
local value = struct.pack('Lc0Lc0LI', string.len(ARGV[1]), ARGV[1], string.len(ARGV[2]), ARGV[2]
, score, tonumber(ARGV[4]));
redis.call('zadd', KEYS[1] , score, value);
redis.call('hset', KEYS[1] , ARGV[2], ARGV[1]);

-- 比对并获取任务
--// 读取并删除任务
--// 1. 通过参数className读取ZSET中第一个任务，比对ID是否相同，如果有任务且相同读取并删除，
--// 2. 解包得到taskId/taskData/executeTime/retryTimes(使用struct.unpack()解包)
--// 3. 根据解包内容taskData和参数className去hash中读取对应的taskId和上一步的taskId比较
--// 4. 如果不同返回null，如果相同，删除hash中的数据，判断hash中是否还有数据，没有删除整个hash，返回解包得到的整个数据

-- key = zsetKey,hashKey
-- val = taskId/taskDataString/executeTime/retryTimes

local expiredValues = redis.call('zrange', KEYS[1], 0, -1);
if #expiredValues > 0 then
    local taskId, taskDataString, executeTime, retryTimes = struct.unpack('Lc0Lc0li4', expiredValues[1]);
    if taskId == ARGV[1] then
        redis.call('zrem', KEYS[1], expiredValues[1]);
        local taskIdInHash = redis.call('hget', KEYS[2], taskDataString);
        if taskId == taskIdInHash then
            redis.call('hdel', KEYS[2], taskDataString);
            return '{\"taskId\":' .. taskId .. ',\"taskDataString\":' .. taskDataString .. ',\"executeTime\":'
                    .. executeTime .. ',\"retryTimes\":' .. retryTimes .. '}';
        end ;
    end ;
end ;
return nil;

-- 刷新头任务,取出ZST的第一条数
-- key = classNameWithPrefix
local expiredValues = redis.call('zrange', KEYS[1], 0, -1);
if #expiredValues > 0 then
    local taskId, taskDataString, executeTime, retryTimes = struct.unpack('Lc0Lc0li4', expiredValues[1]);
    return '{\"taskId\":' .. taskId .. ',\"taskDataString\":' .. taskDataString .. ',\"executeTime\":'
            .. executeTime .. ',\"retryTimes\":' .. retryTimes .. '}';
end ;
return nil;

-- 删除指定数据
-- key = classNameWithPrefix
-- val = taskDataString
