package hudson.plugins.throttleconcurrents.pipeline;

import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.plugins.throttleconcurrents.ThrottleJobProperty;
import hudson.plugins.throttleconcurrents.FlowEntry;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.lang.StringUtils;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.steps.BodyExecutionCallback;
import org.jenkinsci.plugins.workflow.steps.StepContext;
import org.jenkinsci.plugins.workflow.steps.StepExecution;

public class ThrottleStepExecution extends StepExecution {
    private final ThrottleStep step;

    public ThrottleStepExecution(@NonNull ThrottleStep step, StepContext context) {
        super(context);
        this.step = step;
    }

    @NonNull
    public List<String> getCategories() {
        return Collections.unmodifiableList(step.getCategories());
    }
    @NonNull
    public Map<String, Float> getUtilizations() {
        return Collections.unmodifiableMap(step.getUtilizations());
    }

    @Override
    public boolean start() throws Exception {
        Run<?, ?> r = getContext().get(Run.class);
        TaskListener listener = getContext().get(TaskListener.class);
        FlowNode flowNode = getContext().get(FlowNode.class);

        ThrottleJobProperty.DescriptorImpl descriptor = ThrottleJobProperty.fetchDescriptor();

        String runId = null;
        String flowNodeId = null;

        if (r != null && flowNode != null) {
            runId = r.getExternalizableId();
            flowNodeId = flowNode.getId();
            List<String> nonexistent = new ArrayList<String>();
            for (String key : getUtilizations().keySet()) {
                if (descriptor.getCategoryByName(key) == null) {
                    nonexistent.add(key);
                }
            }
            if (nonexistent.isEmpty()) {
                for (Map.Entry<String, Float> kv : getUtilizations().entrySet()) {
                    descriptor.addThrottledPipelineForCategory(runId, new hudson.plugins.throttleconcurrents.FlowEntry(flowNodeId, kv.getKey(), ThrottleJobProperty.toFloat(kv.getValue())), listener);
                }
            } else {
                throw new IllegalArgumentException("One or more specified categories do not exist: " + StringUtils.join(nonexistent, ", "));
            }
        }

        getContext().newBodyInvoker()
                .withCallback(new Callback(runId, flowNodeId, getUtilizations()))
                .start();
        return false;
    }

    @Override
    public void stop(Throwable cause) throws Exception {

    }

    private static final class Callback extends BodyExecutionCallback.TailCall {
        @CheckForNull
        private String runId;
        @CheckForNull
        private String flowNodeId;
        private Map<String, Float> categories = new HashMap<>();


        private static final long serialVersionUID = 1;

        Callback(@CheckForNull String runId, @CheckForNull String flowNodeId, @NonNull Map<String, Float> categories) {
            this.runId = runId;
            this.flowNodeId = flowNodeId;
            this.categories.putAll(categories);
        }

        @Override protected void finished(StepContext context) throws Exception {
            if (runId != null && flowNodeId != null) {
                for (String category : categories.keySet()) {
                    ThrottleJobProperty.fetchDescriptor().removeThrottledPipelineForCategory(runId,
                            flowNodeId,
                            category,
                            context.get(TaskListener.class));
                }
            }
        }
    }
    private static final Logger LOGGER = Logger.getLogger(ThrottleStepExecution.class.getName());
}
