package assignment2;

import java.util.*;
import java.util.concurrent.*;

public class Main {

    public enum TaskType {
        READ,
        WRITE
    }

    public record TaskGroup(UUID groupUUID) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("TaskExecutor argument is null");
            }
        }
    }

    //need to implement for this interface
    public interface TaskExecutor {
        <T> Future<T> submitTask(Main.Task<T> task);
    }

    public record Task<T>(
            UUID taskUUID,
            Main.TaskGroup taskGroup,
            Main.TaskType taskType,
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
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ex) {}
            boolean s = taskGroupUUIDSet.remove(uuid);
            return task;
        }
    }

    //Synchronized Set for keeping track of currently executing Task group, so not to pick task of same group till current group finishes.
    private static Set<UUID> taskGroupUUIDSet = Collections.synchronizedSet(new HashSet<>());

    //class that implements TaskExecutor and does the task execution
    private static class TaskExecutorSvc implements Main.TaskExecutor {

        //number of concurrent tasks to handle
        private static final int concurentCount = 2;

        //ExecutorCompletionService instance to which task are submitted
        private ExecutorCompletionService taskCompletetionSvc;
        //picks the submitted task
        private ExecutorService pickerExecutor;

        public TaskExecutorSvc() {
            pickerExecutor = Executors.newFixedThreadPool(1);
            taskCompletetionSvc = new ExecutorCompletionService(Executors.newFixedThreadPool(concurentCount));
            pickerExecutor.execute(new TaskPicker());
        }

        private volatile boolean isStop = false;

        private void handleExecutorShutdown(ExecutorService executorService) {
            isStop = true;
            executorService.shutdown();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {}
            executorService.shutdownNow();
        }

        public void shutdown() {
            handleExecutorShutdown(pickerExecutor);
        }

        //api exposed , called by client to get task as each task are completed.
        public Task getResult() throws InterruptedException, ExecutionException {
            Task taskCompleted = (Task) taskCompletetionSvc.take().get();//take is BLOCKING call, so if task not completed, it will wait.
            return taskCompleted;
        }

        //Utility class for holding task and it's associated Future, from initial Task submission.
        private static class CompletableFutureTask {
            CompletableFuture future;
            Task task;
            public CompletableFutureTask(Task task, CompletableFuture future) {
                this.future = future;
                this.task = task;
            }
        }

        //Queue to hold task as submitted by client call
        private ConcurrentLinkedQueue<CompletableFutureTask> tasksSubmittedByClient = new ConcurrentLinkedQueue<>();
        //private ConcurrentLinkedQueue<CompletableFutureTask> taskPicked = new ConcurrentLinkedQueue<>();

        /*public class TaskExecutor implements Callable<String> {

            @Override
            public String call() {
                while (true) {
                    CompletableFutureTask cft = taskPicked.poll();
                    if (null != cft) {
                        try {
                            cft.task.taskAction.call();
                            cft.future.complete(cft.task.taskUUID);
                            System.out.println("completed "+cft.task.taskUUID);
                        } catch (Exception ex) {
                            System.out.println(ex.getMessage());
                        } finally {
                            taskGroupUUIDSet.remove(cft.task.taskGroup.groupUUID);
                        }
                    }
                    System.out.println("taskPicked size="+taskPicked.size()+" taskGroupUUIDSet="+taskGroupUUIDSet);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {}
                }
            }
        }*/

        //picks the task from Queue tasksSubmittedByClient and submits to CompletionService
        private class TaskPicker implements Runnable {

            @Override
            public void run() {
                while (!isStop) {
                    CompletableFutureTask cft = tasksSubmittedByClient.peek();
                    if (null != cft) {
                        if (!taskGroupUUIDSet.contains(cft.task.taskGroup.groupUUID)) {
                            tasksSubmittedByClient.poll();
                            taskGroupUUIDSet.add(cft.task.taskGroup.groupUUID);
                            taskCompletetionSvc.submit(cft.task.taskAction);
                            System.out.println(String.format("Task %s of group %s submitted for execution", cft.task.taskUUID, cft.task.taskGroup.groupUUID));
                            //taskPicked.add(cft);
                        } else {
                            System.out.println(String.format("Previous Task of same group %s currently in execution", cft.task.taskGroup.groupUUID));
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {}
                }
            }
        }

        //api implementation of service to accept task
        @Override
        public <T> Future<T> submitTask(Main.Task<T> task) {
            CompletableFuture<T> future = new CompletableFuture<>();
            //new class that will hold this future and task both
            CompletableFutureTask cft = new CompletableFutureTask(task, future);
            tasksSubmittedByClient.add(cft);
            return future;
        }

    }

    public static void main(String[] args) {

        Main.TaskExecutorSvc taskExecutorSvc = new Main.TaskExecutorSvc();
        List<Main.Task> taskList = new ArrayList<>();

        //create 2 group UUID's. more task group can also be used
        Main.TaskGroup taskGroup1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup taskGroup2 = new Main.TaskGroup(UUID.randomUUID());

        //create 10 task, assign Taskgroup to these Tasks
        String taskName = "task";
        for (int i=0;i<10;i++) {
            TaskGroup taskGroup = i%3==0?taskGroup1:taskGroup2;
            int sleep = i%2==0?50:200;
            TaskAction action = new Main.TaskAction(taskName+i, taskGroup.groupUUID, sleep);
            Task task = new Main.Task(UUID.randomUUID(), taskGroup, Main.TaskType.READ, action);
            action.setTask(task);
            taskList.add(task);
        }

        //submit the Task to Service for execution
        List<Future> futures = new ArrayList<>();
        taskList.forEach(task -> {
            Future f = taskExecutorSvc.submitTask(task);
            futures.add(f);
        });

        System.out.println(String.format("Submitted %s futures", futures.size()));

        //wait and pick tasks as completed
        int t = 0;
        while (t<futures.size()) {
            try {
                System.out.println("result=" + taskExecutorSvc.getResult());
                t++;
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        //implement shutdown also
        taskExecutorSvc.shutdown();
        System.out.println("Shutdown");
        System.out.println("Finished executing tasks");
    }

}