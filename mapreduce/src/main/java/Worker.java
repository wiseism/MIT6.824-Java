import com.alibaba.fastjson.JSON;
import common.Cons;
import common.MRArg;
import func.MapFunc;
import func.ReduceFunc;
import java.util.Collections;
import java.util.Set;
import rpc.io.RpcNode;
import util.LogUtil;

/**
 * @author razertory
 * @date 2021/1/1
 */
public class Worker extends RpcNode {

    private CommonMap commonMap = new CommonMap();
    private CommonReduce commonReduce = new CommonReduce();

    public void work(MapFunc mapFunc, ReduceFunc reduceFunc) {
        MRArg mrArg = requireTask(getPort());
        while (mrArg.getWorkerId() != null && !mrArg.getDone() && !Thread.currentThread().isInterrupted()) {
            if (mrArg.getWorkerType().equals(Cons.TASK_TYPE_MAP)) {
                commonMap.doMap(mapFunc, mrArg.getWorkerId(), mrArg.getJobFile(),
                    mrArg.getReduceNum());
            }
            if (mrArg.getWorkerType().equals(Cons.TASK_TYPE_REDUCE)) {
                commonReduce.doReduce(reduceFunc, mrArg.getWorkerId(), mrArg.getMapNum(),
                    mrArg.getReduceOutFile());
            }
            mrArg = doneTask(mrArg.getWorkerId(), mrArg.getWorkerType(), getPort());
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                LogUtil.log("Worker interrupted, exiting work loop");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LogUtil.log("not done yet, " + mrArg);
            }
        }
    }

    private MRArg requireTask(Integer port) {
        Object result = call(Cons.MASTER_HOST, "requireTask", new Object[]{port});
        if (result == null) {
            return MRArg.empty(false);
        }
        try {
            MRArg arg = JSON.parseObject(
                result.toString(),
                MRArg.class);
            return arg;
        } catch (Exception e) {
            LogUtil.log("Failed to parse MRArg: " + e.getMessage());
            return MRArg.empty(false);
        }
    }

    private MRArg doneTask(Integer id, Integer type, Integer port) {
        Object result = call(Cons.MASTER_HOST, "doneTask", new Object[]{id, type, port});
        if (result == null) {
            return MRArg.empty(false);
        }
        try {
            return JSON.parseObject(
                result.toString(),
                MRArg.class);
        } catch (Exception e) {
            LogUtil.log("Failed to parse MRArg: " + e.getMessage());
            return MRArg.empty(false);
        }
    }

    public String rpcPing(String param) {
        return "pong" + param;
    }

    public Set shutDown(String name) {
        shutDownServer();
        Thread.currentThread().interrupt();
        LogUtil.log("interrupted : " + name);
        return Collections.emptySet();
    }
}
