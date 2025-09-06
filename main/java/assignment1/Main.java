/*

following is required

task result should be available as soon as completes - completionexecutionservice
task should be submitted in FIFO order but they may not complete in FIFO order, so needs completionexecutionservice

caller -> submit task -> there should be different completionexecutionservice for each task group, as task in same group can't execute in parallel
-> let them submit and return the CompleteFutures and call task on them to get result.
 */

package assignment1;

import java.util.*;
import java.util.concurrent.*;

public class Main {

    public enum TaskType {
        READ,
        WRITE
    }

    //need to implement for this interface
    public interface TaskExecutor {

        <T> Future<T> submitTask(Task<T> task);

    }

    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("TaskExecutor argument is null");
            }
        }
    }

    public static class TaskAction implements Callable<Task> {
        private String name;
        private UUID uuid;
        private int sleep;
        private Task task;

        TaskAction(String name, UUID uuid, int sleep) {
            this.name = name;
            this.uuid = uuid;
            this.sleep = sleep;
        }

        public void setTask(Task task) {
            this.task = task;
        }

        @Override
        public Task call() {
            outputQueue.add(task);
            System.out.println("Done Executing " + name);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ex) {}
            //System.out.println("Completed "+name+" groupUUID="+uuid+" removed="+s+" taskGroupUUIDSet="+taskGroupUUIDSet);
            return task;
        }
    }

    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("TaskExecutor argument is null");
            }
        }
    }

    //now fit the TaskGroup concept
    //Tasks sharing the same TaskGroup must not run concurrently.
    //for this to be done, some locking mechanism is needed
    //below options come
    //1. there may be fixed number of TaskGroup and we can create those many locks/semaphores to control picking one
    //2. can create collection of taskgroup and have task in them
    //3. let create a Set of UUID and use that for scheduling, else put into queue back
    //4. pick the task, check if that UUID is under processing, if so drop it task to queue else process it.

    private static class TaskExecutorSvc implements TaskExecutor {

        Map<UUID, CompletionService> groupsExecutors = new ConcurrentHashMap<>();
        //number of threads to be controlled depending on how many group's we want to run in parallel.
        //Executor executor = Executors.newFixedThreadPool(4);

        private TaskExecutorSvc() {
        }

        private synchronized CompletionService getCompletionService(Task task) {
            CompletionService svc = groupsExecutors.get(task.taskGroup.groupUUID);
            //this needs to be locked
            if (svc == null) {
                Executor e = Executors.newFixedThreadPool(1);
                svc = new ExecutorCompletionService<>(e);
                groupsExecutors.putIfAbsent(task.taskGroup.groupUUID, svc);
            }

            return svc;
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            //book keep the submitted task and return the future after creating one. future needs to be kept also somewhere
            Future<T> f = getCompletionService(task).submit(task.taskAction);
            return f;
        }

    }

    private static ConcurrentLinkedQueue<Task> outputQueue = new ConcurrentLinkedQueue();

    public static void main(String[] args) {

        TaskExecutorSvc taskExecutorSvc = new TaskExecutorSvc();
        List<Task> taskList = new ArrayList<>();

        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        String taskName = "task";
        for (int i=0;i<10;i++) {
            Main.TaskGroup taskGroup = i%3==0?taskGroup1:taskGroup2;
            int sleep = i%2==0?50:110;
            Main.TaskAction action = new Main.TaskAction(taskName+i, taskGroup.groupUUID, sleep);
            Main.Task task = new Main.Task(UUID.randomUUID(), taskGroup, Main.TaskType.READ, action);
            action.setTask(task);
            taskList.add(task);
        }

        List<Future> futures = new ArrayList<>();
        taskList.forEach(task -> {
            Future f = taskExecutorSvc.submitTask(task);
            futures.add(f);
        });

        System.out.println(String.format("Submitted futures %s", futures.size()));

        //poll the queue and get output
        int t = 0;
        while (t<futures.size()) {
            Task result = outputQueue.poll();
            if (result != null) {
                System.out.println("Result="+result);
                t++;
            }
        }

        System.out.println("Finished executing tasks");

    }

}