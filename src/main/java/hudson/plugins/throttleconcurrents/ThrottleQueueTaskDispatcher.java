package hudson.plugins.throttleconcurrents;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import hudson.Extension;
import hudson.matrix.MatrixConfiguration;
import hudson.matrix.MatrixProject;
import hudson.model.Action;
import hudson.model.Computer;
import hudson.model.Executor;
import hudson.model.Job;
import hudson.model.Node;
import hudson.model.ParameterValue;
import hudson.model.ParametersAction;
import hudson.model.Queue;
import hudson.model.Queue.Task;
import hudson.model.Run;
import hudson.model.labels.LabelAtom;
import hudson.model.queue.CauseOfBlockage;
import hudson.model.queue.QueueTaskDispatcher;
import hudson.model.queue.SubTask;
import hudson.model.queue.WorkUnit;
import hudson.plugins.throttleconcurrents.pipeline.ThrottleStep;
import hudson.security.ACL;
import hudson.security.ACLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import jenkins.model.Jenkins;
import org.codehaus.groovy.runtime.metaclass.MetaMethodIndex;
import org.jenkinsci.plugins.workflow.actions.BodyInvocationAction;
import org.jenkinsci.plugins.workflow.flow.FlowExecution;
import org.jenkinsci.plugins.workflow.flow.FlowExecutionList;
import org.jenkinsci.plugins.workflow.graph.BlockStartNode;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.graph.StepNode;
import org.jenkinsci.plugins.workflow.graphanalysis.LinearBlockHoppingScanner;
import org.jenkinsci.plugins.workflow.steps.StepDescriptor;
import org.jenkinsci.plugins.workflow.support.concurrent.Timeout;
import org.jenkinsci.plugins.workflow.support.steps.ExecutorStepExecution.PlaceholderTask;

@Extension
public class ThrottleQueueTaskDispatcher extends QueueTaskDispatcher {

    @SuppressFBWarnings(value = "MS_SHOULD_BE_FINAL", justification = "deliberately mutable")
    public static boolean USE_FLOW_EXECUTION_LIST =
            Boolean.parseBoolean(
                    System.getProperty(
                            ThrottleQueueTaskDispatcher.class.getName()
                                    + ".USE_FLOW_EXECUTION_LIST",
                            "false"));

    ThrottleJobProperty.DescriptorImpl descriptor = null;

    void fetchDescriptor() {
        if (descriptor == null)  {
            descriptor = ThrottleJobProperty.fetchDescriptor();
        }
        descriptor.processQueues();
    }
    @Deprecated
    @Override
    public @CheckForNull CauseOfBlockage canTake(Node node, Task task) {
        if (!FlowExecutionList.get().isResumptionComplete()) {
            return new CauseOfBlockage() {
                @Override
                public String getShortDescription() {
                    return "Jenkins is resuming operations; wait...";
                }
            };
        }
        if (Jenkins.getAuthentication().equals(ACL.SYSTEM)) {
            return canTakeImpl(node, task);
        }

        // Throttle-concurrent-builds requires READ permissions for all projects.
        try (ACLContext ctx = ACL.as(ACL.SYSTEM)) {
            return canTakeImpl(node, task);
        }
    }
    private CauseOfBlockage canTakeImpl(Node node, Task task) {
        final Jenkins jenkins = Jenkins.get();
        ThrottleJobProperty tjp = getThrottleJobProperty(task);
        fetchDescriptor();
        Map<String, Float> pipelineCategories = categoriesForPipeline(task, descriptor);

        // Handle multi-configuration filters
        if (!shouldBeThrottled(task, tjp, pipelineCategories)) {
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.log(Level.FINER, task.getDisplayName() + ": should not be throttled on node " + node.getDisplayName() + ", tjp: " + tjp);
            }
            return null;
        }

        if (!pipelineCategories.isEmpty() || (tjp != null && tjp.getThrottleEnabled())) {
            CauseOfBlockage cause = canRunImpl(task, tjp, pipelineCategories);
            if (cause != null) {
                return cause;
            }
            if (!pipelineCategories.isEmpty()) {
                if (tjp != null && tjp.getThrottleOption().equals("category")) {
                    pipelineCategories = overrideUtilizations(tjp.getUtilizations(), pipelineCategories);
                }
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.log(Level.FINER, "{0} {1}: canTakeImpl by pipelineCategories: {2}", new Object[]{node.getDisplayName(), task.getDisplayName(), pipelineCategories});
                }
                return throttleCheckForCategories(node, task, jenkins, pipelineCategories, descriptor);
            } else if (tjp != null) {
                if (tjp.getThrottleOption().equals("project")) {
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.log(Level.FINER, "{0}: canTakeImpl by project: {1}", new Object[]{node.getDisplayName(), task.getDisplayName()});
                    }
                    if (tjp.getMaxConcurrentPerNode() > 0) {
                        int maxConcurrentPerNode = tjp.getMaxConcurrentPerNode();
                        int runCount = buildsOfProjectOnNode(node, task, tjp, descriptor);

                        // This would mean that there are as many or more builds currently running than are allowed.
                        if (runCount >= maxConcurrentPerNode) {
                            return CauseOfBlockage.fromMessage(
                                    Messages._ThrottleQueueTaskDispatcher_MaxCapacityOnNode(runCount));
                        }
                    }
                } else if (tjp.getThrottleOption().equals("category")) {
                    Map<String, Float> utilizations = overrideUtilizations(tjp.getUtilizations(), pipelineCategories);
                    if (LOGGER.isLoggable(Level.FINER)) {
                        LOGGER.log(Level.FINER, "{0} {1}: canTakeImpl by category: {2}", new Object[]{node.getDisplayName(), task.getDisplayName(), utilizations});
                }
                    return throttleCheckForCategories(node, task, jenkins, utilizations, descriptor);
            }
        }
        }

        return null;
    }

    private CauseOfBlockage throttleCheckForCategories(Node node, Task task, Jenkins jenkins, Map<String, Float> categories, ThrottleJobProperty.DescriptorImpl descriptor) {
        boolean allNodes = node == null;
        if (LOGGER.isLoggable(Level.FINE)) {
            if (allNodes) {
                LOGGER.log(Level.FINE, task.getFullDisplayName() + ": Looking at all nodes");
            } else {
                LOGGER.log(Level.FINE, task.getFullDisplayName() + ": Looking at node " + node.getDisplayName());
            }
        }
        for (String catNm : categories.keySet()) {
                if (catNm != null && !catNm.equals("")) {
                List<Task> categoryTasks = ThrottleJobProperty.getCategoryTasks(catNm, descriptor);

                    ThrottleJobProperty.ThrottleCategory category =
                        descriptor.getCategoryByName(catNm);

                    if (category != null) {
                    Map<String, Float> runCount = new HashMap<>();
                    int maxConcurrent = allNodes ? category.getMaxConcurrentTotal() : getMaxConcurrentPerNodeBasedOnMatchingLabels(
                                node, category, category.getMaxConcurrentPerNode());
                    
                    if (maxConcurrent > 0) {
                        if (LOGGER.isLoggable(Level.FINER)) {
                            LOGGER.log(Level.FINER, task.getFullDisplayName() + (allNodes ? ": max concurrent over all nodes is " : ": max concurrent per node is ") + maxConcurrent);
                        }
                        if (LOGGER.isLoggable(Level.FINEST)) {
                            LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": Looking at category tasks: " + categoryTasks.stream().map(Object::toString).collect(Collectors.joining(", ")));
                        }
                            for (Task catTask : categoryTasks) {
                            /*if (catTask.equals(task.getOwnerTask()) && (task.getOwnerTask() != task)) {
                                LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": Skipping parent category task: " + catTask);
                                continue;
                            }*/

                            /*if (jenkins.getQueue().isPending(catTask)) {
                                LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": Skipping pending category task: " + catTask);
                                continue;
                            }*/
                                if (jenkins.getQueue().isPending(catTask)) {
                                    return CauseOfBlockage.fromMessage(
                                            Messages._ThrottleQueueTaskDispatcher_BuildPending());
                                }
                            int buildCount = allNodes ? buildsOfProjectOnAllNodes(catTask, getThrottleJobProperty(catTask), descriptor) : buildsOfProjectOnNode(node, catTask, getThrottleJobProperty(catTask), descriptor);
                            if (buildCount != 0) {
                                if (LOGGER.isLoggable(Level.FINEST)) {
                                    LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": freestyle build count for " + catTask + " is " + buildCount);
                            }
                                computeCurrentUtilization(task, runCount, catTask, buildCount, descriptor);
                            }
                        }
                        Map<String,List<FlowNode>> throttledPipelines = ThrottleJobProperty.getThrottledPipelineRunsForCategory(catNm, descriptor);
                        if (LOGGER.isLoggable(Level.FINEST)) {
                            LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": Looking at pipeline tasks: " + throttledPipelines.entrySet().stream().map(Object::toString).collect(Collectors.joining(", ")));
                        }
                        for (Map.Entry<String,List<FlowNode>> entry : throttledPipelines.entrySet()) {
                                if (hasPendingPipelineForCategory(entry.getValue())) {
                                    return CauseOfBlockage.fromMessage(
                                            Messages._ThrottleQueueTaskDispatcher_BuildPending());
                                }
                            Run<?,?> r = Run.fromExternalizableId(entry.getKey());
                            if (r != null && r.isBuilding()) {
                                List<FlowNode> pipelines = allNodes ? getPipelinesOnAllNodes(r, entry.getValue()) : getPipelinesOnNode(node, r, entry.getValue());
                                if (LOGGER.isLoggable(Level.FINEST) && pipelines.size() != 0) {
                                    LOGGER.log(Level.FINEST, task.getFullDisplayName() + ": pipeline build count for " + r + " is " + pipelines.size());
                                }
                                computeCurrentUtilization2(task, pipelines, runCount, r, descriptor);
                            }
                        }
                        if (evaluateBlockage(task, runCount, maxConcurrent, categories)) {
                            return CauseOfBlockage.fromMessage(allNodes ? Messages._ThrottleQueueTaskDispatcher_MaxCapacityTotal(runCount) : Messages._ThrottleQueueTaskDispatcher_MaxCapacityOnNode(runCount));
                        }
                    }
                } 
            }
        }
        return null;
    }

    private int roundToPrecision(float f) {
        return Math.round(f * 100) / 100;
    }

    private boolean evaluateBlockage(Task pendingTask, Map<String, Float> runCount, int limit, Map<String, Float> categories) {
        //LOGGER.log(Level.INFO, pendingTask.getDisplayName() + ": run counts:");
        //for (Map.Entry<String, Float> e : runCount.entrySet()) {
            //LOGGER.log(Level.INFO, pendingTask.getDisplayName() + ": " + e.getKey() + ": " + e.getValue());
        //}
        if (runCount != null && runCount.entrySet().stream().anyMatch(v -> roundToPrecision(v.getValue() + categories.getOrDefault(v.getKey(), 0.0f)) > limit)) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, pendingTask.getDisplayName() + ": max capacity reached: " + runCount + ", limit: " + limit);
            }
            return true;
        } else {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, pendingTask.getDisplayName() + ": max capacity NOT reached: " + runCount + ", limit: " + limit);
            }
        }
        return false;
    }

    private void computeCurrentUtilization2(Task pendingTask, List<FlowNode> pipelines, Map<String, Float> runCount, Run<?,?> r, ThrottleJobProperty.DescriptorImpl descriptor) {
                                    if (r.isBuilding()) {
            //LOGGER.log(Level.INFO, pendingTask.getDisplayName() + ": isBuilding == true");
            if (pipelines != null && pipelines.size() != 0) {
                ThrottleJobProperty tjp = getThrottleJobProperty(pendingTask);
                for (FlowNode fn : pipelines) {
                    Map<String, Float> cats = new HashMap<>();
                    if (tjp != null && tjp.getThrottleEnabled() && tjp.getThrottleOption().equals("category")) {
                        cats = tjp.getUtilizations();
                                    }
                    cats = overrideUtilizations(cats, categoriesForPipeline(r, fn, descriptor));
                    for (Map.Entry<String, Float> kv : cats.entrySet()) {
                        runCount.put(kv.getKey(), kv.getValue() + runCount.getOrDefault(kv.getKey(), 0.0f));
                                }
                            }
                            }
                        }
                    }

    private void computeCurrentUtilization(Task pendingTask, Map<String, Float> runCount, Task runningTask, int multiplier, ThrottleJobProperty.DescriptorImpl descriptor) {
        Map<String, Float> pipeCats = categoriesForPipeline(runningTask, descriptor);
        for (Map.Entry<String, Float> kv : pipeCats.entrySet()) {
            //LOGGER.log(Level.INFO, pendingTask.getDisplayName() + ": other pipeline build cat: " + kv.getKey() + ": " + kv.getValue());
            runCount.put(kv.getKey(), kv.getValue() * multiplier + runCount.getOrDefault(kv.getKey(), 0.0f));
                }
        ThrottleJobProperty prop = getThrottleJobProperty(runningTask);
        for (Map.Entry<String, Float> kv : prop.getUtilizations().entrySet()) {
            if (pipeCats.containsKey(kv.getKey())) {
                continue;
            }
            //LOGGER.log(Level.INFO, pendingTask.getDisplayName() + ": other freestyle build cat: " + kv.getKey() + ": " + kv.getValue());
            runCount.put(kv.getKey(), kv.getValue() * multiplier + runCount.getOrDefault(kv.getKey(), 0.0f));
        }

    }

    private boolean hasPendingPipelineForCategory(List<FlowNode> flowNodes) {
        for (Queue.BuildableItem pending : Jenkins.get().getQueue().getPendingItems()) {
            if (firstThrottleStartNode(pending.task, flowNodes) != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public @CheckForNull CauseOfBlockage canRun(Queue.Item item) {
        if (!FlowExecutionList.get().isResumptionComplete()) {
            return new CauseOfBlockage() {
                @Override
                public String getShortDescription() {
                    return "Jenkins is resuming operations; wait...";
                }
            };
        }
        fetchDescriptor();
        ThrottleJobProperty tjp = getThrottleJobProperty(item.task);
        Map<String, Float> pipelineCategories = categoriesForPipeline(item.task, descriptor);

        if (!pipelineCategories.isEmpty() || (tjp != null && tjp.getThrottleEnabled())) {
            if (tjp != null
                    && tjp.isLimitOneJobWithMatchingParams()
                    && isAnotherBuildWithSameParametersRunningOnAnyNode(item)) {
                return CauseOfBlockage.fromMessage(
                        Messages._ThrottleQueueTaskDispatcher_OnlyOneWithMatchingParameters());
            }
            //LOGGER.log(Level.INFO, "canRun for Queue.Item {0}", item.task.getDisplayName());
            return canRun(item.task, tjp, pipelineCategories);
        }
        if (LOGGER.isLoggable(Level.FINER)) {
            LOGGER.log(Level.FINER, item.getDisplayName() + ": should not be throttled, tjp: " + tjp);
        }
        return null;
    }

    @NonNull
    private ThrottleMatrixProjectOptions getMatrixOptions(Task task) {
        ThrottleJobProperty tjp = getThrottleJobProperty(task);

        if (tjp == null) {
            return ThrottleMatrixProjectOptions.DEFAULT;
        }
        ThrottleMatrixProjectOptions matrixOptions = tjp.getMatrixOptions();
        return matrixOptions != null ? matrixOptions : ThrottleMatrixProjectOptions.DEFAULT;
    }
    private boolean shouldBeThrottled(@NonNull Task task, @CheckForNull ThrottleJobProperty tjp, Map<String, Float> pipelineCategories) {
        if (pipelineCategories != null && !pipelineCategories.isEmpty()) {
            return true;
        }
        if (tjp == null) {
            return false;
        }
        if (!tjp.getThrottleEnabled()) {
            return false;
        }

        // Handle matrix options
        ThrottleMatrixProjectOptions matrixOptions = tjp.getMatrixOptions();
        if (matrixOptions == null) {
            matrixOptions = ThrottleMatrixProjectOptions.DEFAULT;
        }
        if (!matrixOptions.isThrottleMatrixConfigurations() && task instanceof MatrixConfiguration) {
            return false;
        }
        if (!matrixOptions.isThrottleMatrixBuilds() && task instanceof MatrixProject) {
            return false;
        }

        // Allow throttling by default
        return true;
    }
    private boolean shouldBeThrottled(@NonNull Task task, @CheckForNull ThrottleJobProperty tjp, ThrottleJobProperty.DescriptorImpl descriptor) {
        return shouldBeThrottled(task, tjp, categoriesForPipeline(task, descriptor));
    }

    private CauseOfBlockage canRun(Task task, ThrottleJobProperty tjp, Map<String, Float> pipelineCategories) {
        if (Jenkins.getAuthentication2().equals(ACL.SYSTEM2)) {
            return canRunImpl(task, tjp, pipelineCategories);
        }

        // Throttle-concurrent-builds requires READ permissions for all projects.
        try (ACLContext ctx = ACL.as2(ACL.SYSTEM2)) {
            return canRunImpl(task, tjp, pipelineCategories);
        }
    }

    private CauseOfBlockage canRunImpl(Task task, ThrottleJobProperty tjp, Map<String, Float> pipelineCategories) {
        final Jenkins jenkins = Jenkins.get();
        if (!shouldBeThrottled(task, tjp, pipelineCategories)) {
            return null;
        }
        fetchDescriptor();
        if (jenkins.getQueue().isPending(task)) {
            return CauseOfBlockage.fromMessage(Messages._ThrottleQueueTaskDispatcher_BuildPending());
        }
        if (!pipelineCategories.isEmpty()) {
            if (tjp != null && tjp.getThrottleOption().equals("category")) {
                pipelineCategories = overrideUtilizations(tjp.getUtilizations(), pipelineCategories);
            }
            if (LOGGER.isLoggable(Level.FINER)) {
                LOGGER.log(Level.FINER, "{0}: canRunImpl by pipelineCategories: {1}", new Object[]{task.getDisplayName(), pipelineCategories});
            }
            return throttleCheckForCategories(null, task, jenkins, pipelineCategories, descriptor);
        } else if (tjp != null) {
            if (tjp.getThrottleOption().equals("project")) {
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.log(Level.FINER, "{0}: canRunImpl by project", task.getDisplayName());
                }
                if (tjp.getMaxConcurrentTotal() > 0) {
                    int maxConcurrentTotal = tjp.getMaxConcurrentTotal();
                    int totalRunCount = buildsOfProjectOnAllNodes(task, tjp, descriptor);

                    if (totalRunCount >= maxConcurrentTotal) {
                        return CauseOfBlockage.fromMessage(
                                Messages._ThrottleQueueTaskDispatcher_MaxCapacityTotal(totalRunCount));
                    }
                }
            } else if (tjp.getThrottleOption().equals("category")) {
                Map<String, Float> utilizations = overrideUtilizations(tjp.getUtilizations(), pipelineCategories);
                if (LOGGER.isLoggable(Level.FINER)) {
                    LOGGER.log(Level.FINER, "{0}: canRunImpl by category: {1}", new Object[]{task.getDisplayName(), utilizations});
            }
                return throttleCheckForCategories(null, task, jenkins, utilizations, descriptor);
        }
        }
        return null;
    }

    Map<String, Float> overrideUtilizations(Map<String, Float> map1, Map<String, Float> map2) {
        Map<String, Float> utilizations = new HashMap<String, Float>(map1);
        for (Map.Entry<String, Float> e : map2.entrySet()) {
            utilizations.put(e.getKey(), e.getValue());
                            }
        return utilizations;
                        }

    private boolean isAnotherBuildWithSameParametersRunningOnAnyNode(Queue.Item item) {
        final Jenkins jenkins = Jenkins.get();
        if (isAnotherBuildWithSameParametersRunningOnNode(jenkins, item)) {
            return true;
        }

        for (Node node : jenkins.getNodes()) {
            if (isAnotherBuildWithSameParametersRunningOnNode(node, item)) {
                return true;
            }
        }
        return false;
    }

    private boolean isAnotherBuildWithSameParametersRunningOnNode(Node node, Queue.Item item) {
        ThrottleJobProperty tjp = getThrottleJobProperty(item.task);
        if (tjp == null) {
            // If the property has been occasionally deleted by this call,
            // it does not make sense to limit the throttling by parameter.
            return false;
        }
        Computer computer = node.toComputer();
        List<String> paramsToCompare = tjp.getParamsToCompare();
        List<ParameterValue> itemParams = getParametersFromQueueItem(item);

        if (paramsToCompare.size() > 0) {
            itemParams = doFilterParams(paramsToCompare, itemParams);
        }

        // Look at all executors of specified node => computer,
        // and what work units they are busy with (if any) - and
        // whether one of these executing units is an instance
        // of the queued item we were asked to compare to.
        if (computer != null) {
            for (Executor exec : computer.getExecutors()) {
                // TODO: refactor into a nameEquals helper method
                final Queue.Executable currentExecutable = exec.getCurrentExecutable();
                final SubTask parentTask = currentExecutable != null ? currentExecutable.getParent() : null;
                if (currentExecutable != null
                        && parentTask.getOwnerTask().getName().equals(item.task.getName())) {
                    List<ParameterValue> executingUnitParams = getParametersFromWorkUnit(exec.getCurrentWorkUnit());
                    executingUnitParams = doFilterParams(paramsToCompare, executingUnitParams);

                    // An already executing work unit (of the same name) can have more
                    // parameters than the queued item, e.g. due to env injection or by
                    // unfiltered inheritance of "unsupported officially" from a caller.
                    // Note that similar inheritance can also get more parameters into
                    // the queued item than is visibly declared in its job configuration.
                    // Normally the job configuration should declare all params that are
                    // listed in its throttle configuration. Jenkins may forbid passing
                    // undeclared parameters anyway, due to security concerns by default.
                    // We check here whether the interesting (or all, if not filtered)
                    // specified params of the queued item are same (glorified key=value
                    // entries) as ones used in a running work unit, in any order.
                    if (executingUnitParams.containsAll(itemParams)) {
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.log(Level.FINE, "build (" + exec.getCurrentWorkUnit() +
                                    ") with identical parameters (" +
                                    executingUnitParams + ") is already running.");
                        }
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Filter job parameters to only include parameters used for throttling
     * @param params - a list of Strings with parameter names to compare
     * @param OriginalParams - a list of ParameterValue descendants whose name fields should match
     * @return a list of ParameterValue descendants whose name fields did match, entries copied from OriginalParams
     */
    private List<ParameterValue> doFilterParams(List<String> params, List<ParameterValue> OriginalParams) {
        if (params.isEmpty()) {
            return OriginalParams;
        }

        List<ParameterValue> newParams = new ArrayList<>();

        for (ParameterValue p : OriginalParams) {
            if (params.contains(p.getName())) {
                newParams.add(p);
            }
        }
        return newParams;
    }

    public List<ParameterValue> getParametersFromWorkUnit(WorkUnit unit) {
        List<ParameterValue> paramsList = new ArrayList<>();

        if (unit != null && unit.context != null) {
            if (unit.context.actions != null && !unit.context.actions.isEmpty()) {
                List<Action> actions = unit.context.actions;
                for (Action action : actions) {
                    if (action instanceof ParametersAction) {
                        paramsList = ((ParametersAction) action).getParameters();
                    }
                }
            } else if (unit.context.task instanceof PlaceholderTask) {
                PlaceholderTask placeholderTask = (PlaceholderTask) unit.context.task;
                Run<?, ?> run = placeholderTask.run();
                if (run != null) {
                    List<ParametersAction> actions = run.getActions(ParametersAction.class);
                    for (ParametersAction action : actions) {
                        paramsList = action.getParameters();
                    }
                }
            }
        }
        return paramsList;
    }

    public List<ParameterValue> getParametersFromQueueItem(Queue.Item item) {
        List<ParameterValue> paramsList;

        ParametersAction params = item.getAction(ParametersAction.class);
        if (params != null) {
            paramsList = params.getParameters();
        } else {
            paramsList = new ArrayList<>();
        }
        return paramsList;
    }

    @NonNull
    private Map<String, Float> categoriesForPipeline(Task task, ThrottleJobProperty.DescriptorImpl descriptor) {
        if (task instanceof PlaceholderTask) {
            PlaceholderTask placeholderTask = (PlaceholderTask) task;
            Run<?, ?> r = placeholderTask.run();
            if (r != null) {
                Map<String, Map<String, Float>> categoriesByFlowNode = ThrottleJobProperty.getCategoriesForRunByFlowNode(r, descriptor);
                if (!categoriesByFlowNode.isEmpty()) {
                    try (Timeout t = Timeout.limit(100, TimeUnit.MILLISECONDS)) {
                        FlowNode firstThrottle = firstThrottleStartNode(placeholderTask.getNode());
                        if (firstThrottle != null) {
                            Map<String, Float> categories = categoriesByFlowNode.get(firstThrottle.getId());
                            if (categories != null) {
                                return categories;
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "Error getting categories for pipeline {0}: {1}",
                                    new Object[]{task.getDisplayName(), e});
                    }
                        return new HashMap<>();
                }
            }
        }
    }
        return new HashMap<>();
    }

    @NonNull
    private Map<String, Float> categoriesForPipeline(Run<?,?> r, FlowNode fn, ThrottleJobProperty.DescriptorImpl descriptor) {
        if (r != null) {
            Map<String, Map<String, Float>> categoriesByFlowNode = ThrottleJobProperty.getCategoriesForRunByFlowNode(r, descriptor);
            if (!categoriesByFlowNode.isEmpty()) {
                try (Timeout t = Timeout.limit(100, TimeUnit.MILLISECONDS)) {
                    FlowNode firstThrottle = firstThrottleStartNode(fn);
                    if (firstThrottle != null) {
                        Map<String, Float> categories = categoriesByFlowNode.get(firstThrottle.getId());
                        if (categories != null) {
                            return categories;
                        }
                    }
                } 
            }
        }
        return new HashMap<>();
    }

    @CheckForNull
    private ThrottleJobProperty getThrottleJobProperty(Task task) {
        Job<?,?> p = null;
        ThrottleJobProperty prop = null;
        if (task instanceof Job) {
            p = (Job<?,?>) task;
            if (task instanceof MatrixConfiguration) {
                p = ((MatrixConfiguration) task).getParent();
            }
        } else if (task instanceof PlaceholderTask) {
            //LOGGER.log(Level.INFO, "Placeholdertask {0}", new Object[] { task });
            p = (Job<?,?>)((PlaceholderTask)task).getOwnerTask();
        }
        if (p != null) {
            prop = p.getProperty(ThrottleJobProperty.class);
    }
        if (prop == null) {
            //LOGGER.log(Level.INFO, "tjp is null for task: {0}, {1}", new Object[] { task, task.getClass().getName() });
        }
        return prop;
    }

    private int countPipelinesOnNode(@NonNull Node node, @NonNull Run<?,?> run, @NonNull List<FlowNode> flowNodes) {
        return getPipelinesOnNode(node, run, flowNodes).size();
    }

    private List<FlowNode> getPipelinesOnNode(@NonNull Node node, @NonNull Run<?,?> run, @NonNull List<FlowNode> flowNodes) {
        List<FlowNode> nodes = new ArrayList<FlowNode>();
        //LOGGER.log(Level.FINE, "Checking for pipelines of {0} on node {1}", new Object[] {run.getDisplayName(), node.getDisplayName()});

        Computer computer = node.toComputer();
        if (computer != null) { // Not all nodes are certain to become computers, like nodes with 0 executors.
            // Don't count flyweight tasks that might not consume an actual executor, unlike with builds.
            for (Executor e : computer.getExecutors()) {
                FlowNode n = pipelinesOnExecutor(run, e, flowNodes);
                if (n != null) {
                    nodes.add(n);
            }
        }
        }

        return nodes;
    }


    private List<FlowNode> getPipelinesOnAllNodes(@NonNull Run<?,?> run, @NonNull List<FlowNode> flowNodes) {
        final Jenkins jenkins = Jenkins.get();
        List<FlowNode> fns = getPipelinesOnNode(jenkins, run, flowNodes);

        for (Node node : jenkins.getNodes()) {
            fns.addAll(getPipelinesOnNode(node, run, flowNodes));
        }
        return fns;
    }

    private int buildsOfProjectOnNode(Node node, Task task, ThrottleJobProperty tjp, ThrottleJobProperty.DescriptorImpl descriptor) {
        if (!shouldBeThrottled(task, tjp, descriptor)) {
            //LOGGER.log(Level.INFO, "buildsOfProjectOnNode: shouldBeThrottled: false, for task {0}", new Object[] { task });
            return 0;
        }

        // Note that this counts flyweight executors in its calculation, which may be a problem if
        // flyweight executors are being leaked by other plugins.
        return buildsOfProjectOnNodeImpl(node, task);
    }

    private int buildsOfProjectOnAllNodes(Task task, ThrottleJobProperty tjp, ThrottleJobProperty.DescriptorImpl descriptor) {
        if (!shouldBeThrottled(task, tjp, descriptor)) {
            //LOGGER.log(Level.INFO, "buildsOfProjectOnAllNodes: shouldBeThrottled: false, for task {0}", new Object[] { task });
            return 0;
        }

        // Note that we can't use WorkflowJob.class because it is not on this plugin's classpath.
        if (USE_FLOW_EXECUTION_LIST
                && task.getClass().getName().equals("org.jenkinsci.plugins.workflow.job.WorkflowJob")) {
            return buildsOfPipelineJob(task);
        } else {
            return buildsOfProjectOnAllNodesImpl(task, tjp, descriptor);
        }
    }

    private int buildsOfPipelineJob(Task task) {
        //LOGGER.log( Level.INFO, task.getDisplayName() + "buildsOfPipelineJob");
        int runCount = 0;
        for (FlowExecution flowExecution : FlowExecutionList.get()) {
            if (isExecutionOfTask(task, flowExecution)) {
                    runCount++;
                }
        }
        if (runCount != 0 && LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "{0}: FlowExecutionList builds: {2}", new Object[] {task, runCount});
        }

        return runCount;
    }

    private boolean isExecutionOfTask(Task task, FlowExecution flowExecution) {
        try {
            return (isExecutionOfTask(task, flowExecution.getOwner().getExecutable()));
            } catch (IOException e) {
            if (LOGGER.isLoggable(Level.FINEST)) { // Performance critical area
                LOGGER.log(
                        Level.FINEST,
                        "Error getting number of builds for pipeline {0}: {1}",
                        new Object[]{task.getDisplayName(), e});
            }
        }
        return false;
    }
    private boolean isExecutionOfTask(Task task, Queue.Executable executable) {
        if (executable == null) {
            return false;
        }
        final SubTask parent = executable.getParent();
        if (task.equals(parent)) {
            return true;
        }
        final SubTask pot = parent.getOwnerTask();
        if (task.equals(pot)) {
            return true;
        }
        final SubTask tot = task.getOwnerTask();
        //LOGGER.log(Level.INFO, "{0}: getOwnerTask: {1}, parent: {2}, in queue: {3}", new Object[] {task.getDisplayName(), parent.getDisplayName(), parent.getOwnerTask().getDisplayName(), Queue.getInstance().contains(parent.getOwnerTask())});
        return tot.equals(pot) || tot.equals(parent);
    }

    private int buildsOfProjectOnNodeImpl(Node node, Task task) {
        int runCount = 0;
        //LOGGER.log(Level.INFO, "Checking for builds of {0} on node {1}", new Object[] {task.getName(), node.getDisplayName()});

        // I think this'll be more reliable than job.getBuilds(), which seemed to not always get
        // a build right after it was launched, for some reason.
        Computer computer = node.toComputer();
        if (computer != null) { // Not all nodes are certain to become computers, like nodes with 0 executors.
            // Count flyweight tasks that might not consume an actual executor.
            /*for (Executor e : computer.getOneOffExecutors()) {
                int n = buildsOnExecutor(task, e);
                //LOGGER.log(Level.FINEST, "{0}: one-off builds on node {1}: {2}", new Object[] {task.getName(), node.getDisplayName(), n});
                runCount += n;
            }*/

            for (Executor e : computer.getExecutors()) {
                int n = buildsOnExecutor(task, e);
                if (n != 0 && LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.log(Level.FINEST, "{0}: normal builds on node {1}: {2}", new Object[] {task, node.getDisplayName(), n});
            }
                runCount += n;
        }
        }
        return runCount;
    }

    private int buildsOfProjectOnAllNodesImpl(Task task, ThrottleJobProperty tjp, ThrottleJobProperty.DescriptorImpl descriptor) {
        //LOGGER.log(Level.INFO, task.getDisplayName() + "buildsOfProjectOnAllNodesImpl");
        final Jenkins jenkins = Jenkins.get();
        int totalRunCount = buildsOfProjectOnNode(jenkins, task, tjp, descriptor);

        for (Node node : jenkins.getNodes()) {
            totalRunCount += buildsOfProjectOnNodeImpl(node, task);
        }
        return totalRunCount;
    }

    private int buildsOnExecutor(Task task, Executor exec) {
        if (isExecutionOfTask(task, exec.getCurrentExecutable())) {
            return 1;
        }
        return 0;
    }

    /**
     * Get the count of currently executing {@link PlaceholderTask}s on a given {@link Executor} for a given {@link Run}
     * and list of {@link FlowNode}s in that run that have been throttled.
     *
     * @param run The {@link Run} we care about.
     * @param exec The {@link Executor} we're checking on.
     * @param flowNodes The list of {@link FlowNode}s associated with that run that have been throttled with a particular
     *                  category.
     * @return 1 if there's something currently executing on that executor and it's of that run and one of the provided
     * flow nodes, 0 otherwise.
     */
    private FlowNode pipelinesOnExecutor(@NonNull Run<?,?> run, @NonNull Executor exec, @NonNull List<FlowNode> flowNodes) {
        final Queue.Executable currentExecutable = exec.getCurrentExecutable();
        if (currentExecutable != null) {
            SubTask parent = currentExecutable.getParent();
            if (parent instanceof PlaceholderTask) {
                PlaceholderTask task = (PlaceholderTask) parent;
                if (run.equals(task.run())) {
                    return firstThrottleStartNode(task, flowNodes);
                    }
                }
            }
        return null;
        }

    private FlowNode firstThrottleStartNode(Task origTask, List<FlowNode> flowNodeFilter) {
        if (origTask instanceof PlaceholderTask) {
            PlaceholderTask task = (PlaceholderTask) origTask;
            try {
                FlowNode firstThrottle = firstThrottleStartNode(task.getNode());
                if (firstThrottle != null && flowNodeFilter.contains(firstThrottle)) {
                    return firstThrottle;
                }
            } catch (IOException | InterruptedException e) {
                // TODO: do something?
            }
        }

        return null;
    }

    /**
     * Given a {@link FlowNode}, find the {@link FlowNode} most directly enclosing this one that comes from a {@link ThrottleStep}.
     *
     * @param inner The inner {@link FlowNode}
     * @return The most immediate enclosing {@link FlowNode} of the inner one that is associated with {@link ThrottleStep}. May be null.
     */
    @CheckForNull
    private FlowNode firstThrottleStartNode(@CheckForNull FlowNode inner) {
        if (inner != null) {
            LinearBlockHoppingScanner scanner = new LinearBlockHoppingScanner();
            scanner.setup(inner);
            for (FlowNode enclosing : scanner) {
                if (enclosing != null
                        && enclosing instanceof BlockStartNode
                        && enclosing instanceof StepNode
                        &&
                        // There are two BlockStartNodes (aka StepStartNodes) for ThrottleStep, so make sure we get the
                        // first one of those two, which will not have BodyInvocationAction.class on it.
                        enclosing.getAction(BodyInvocationAction.class) == null) {
                    // Check if this is a *different* throttling node.
                    StepDescriptor desc = ((StepNode) enclosing).getDescriptor();
                    if (desc != null && desc.getClass().equals(ThrottleStep.DescriptorImpl.class)) {
                        return enclosing;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @param node to compare labels with.
     * @param category to compare labels with.
     * @param maxConcurrentPerNode to return if node labels mismatch.
     * @return maximum concurrent number of builds per node based on matching labels, as an int.
     * @author marco.miller@ericsson.com
     */
    private int getMaxConcurrentPerNodeBasedOnMatchingLabels(
            Node node, ThrottleJobProperty.ThrottleCategory category, int maxConcurrentPerNode) {
        List<ThrottleJobProperty.NodeLabeledPair> nodeLabeledPairs = category.getNodeLabeledPairs();
        int maxConcurrentPerNodeLabeledIfMatch = maxConcurrentPerNode;
        boolean nodeLabelsMatch = false;
        Set<LabelAtom> nodeLabels = node.getAssignedLabels();

        for (ThrottleJobProperty.NodeLabeledPair nodeLabeledPair : nodeLabeledPairs) {
            String throttledNodeLabel = nodeLabeledPair.getThrottledNodeLabel();
            if (!nodeLabelsMatch && !throttledNodeLabel.isEmpty()) {
                for (LabelAtom aNodeLabel : nodeLabels) {
                    String nodeLabel = aNodeLabel.getDisplayName();
                    if (nodeLabel.equals(throttledNodeLabel)) {
                        maxConcurrentPerNodeLabeledIfMatch = nodeLabeledPair.getMaxConcurrentPerNodeLabeled();
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.log(Level.FINE, "node labels match; => maxConcurrentPerNode'' = {0}", maxConcurrentPerNodeLabeledIfMatch);
                        }
                        nodeLabelsMatch = true;
                        break;
                    }
                }
            }
        }
        if(!nodeLabelsMatch && LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("node labels mismatch");
        }
        return maxConcurrentPerNodeLabeledIfMatch;
    }

    private static final Logger LOGGER = Logger.getLogger(ThrottleQueueTaskDispatcher.class.getName());
}
