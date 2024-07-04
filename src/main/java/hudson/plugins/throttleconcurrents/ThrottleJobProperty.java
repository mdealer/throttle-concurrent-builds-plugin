package hudson.plugins.throttleconcurrents;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.Util;
import hudson.matrix.MatrixConfiguration;
import hudson.matrix.MatrixProject;
import hudson.matrix.MatrixRun;
import hudson.model.AbstractDescribableImpl;
import hudson.model.Descriptor;
import hudson.model.Item;
import hudson.model.ItemGroup;
import hudson.model.Job;
import hudson.model.JobProperty;
import hudson.model.JobPropertyDescriptor;
import hudson.model.Queue;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.util.CopyOnWriteMap;
import hudson.util.CopyOnWriteMap.Tree;
import hudson.util.FormValidation;
import hudson.util.ListBoxModel;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.lang.model.type.UnionType;

import edu.umd.cs.findbugs.annotations.CheckForNull;
import edu.umd.cs.findbugs.annotations.NonNull;
import jenkins.model.Jenkins;
import net.sf.json.JSONObject;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.diff.StringsComparator;
import org.jenkinsci.Symbol;
import org.jenkinsci.plugins.workflow.flow.FlowExecution;
import org.jenkinsci.plugins.workflow.flow.FlowExecutionOwner;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.kohsuke.stapler.AncestorInPath;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;
import org.kohsuke.stapler.StaplerRequest;

public class ThrottleJobProperty extends JobProperty<Job<?, ?>> {
    // Replaced by categories, to support, well, multiple categories per job (starting from 1.3)
    @Deprecated
    transient String category;

    private Integer maxConcurrentPerNode;
    private Integer maxConcurrentTotal;
    private List<String> categories;
    private boolean throttleEnabled;
    private String throttleOption;
    private boolean limitOneJobWithMatchingParams;
    private transient boolean throttleConfiguration;
    private @CheckForNull ThrottleMatrixProjectOptions matrixOptions;

    // The paramsToUseForLimit is assigned by end-user configuration and
    // is generally a string with names of build arguments to consider,
    // and is empty, or has one arg name, or a token-separated list of
    // such names (see PARAMS_LIMIT_SEPARATOR below).
    // The paramsToCompare is an array of arg name strings, one per
    // list entry, processed from paramsToUseForLimit.
    private String paramsToUseForLimit;
    private transient List<String> paramsToCompare;

    /*
     * Documentation only stated "," but its use was broken for so long that probably people used
     * the de-facto working whitespace instead.
     */
    private static final String PARAMS_LIMIT_SEPARATOR = "[\\s,]+";

    /**
     * Store a config version so we're able to migrate config on various
     * functionality upgrades.
     */
    private Long configVersion;

    /**
     * Returns a merge function, suitable for use in {@link Map#merge(Object, Object,
     * BiFunction) Map.merge()} or {@link Collectors#toMap(Function, Function, BinaryOperator)
     * toMap()}, which always throws {@code IllegalStateException}. This can be used to enforce
     * the assumption that the elements being collected are distinct.
     *
     * @param <T> the type of input arguments to the merge function
     * @return a merge function which always throw {@code IllegalStateException}
     */
    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }
    
    @SuppressWarnings("unchecked")
    protected static Map<String, Float> convertCategoryObjectToMap(Object e) {
        if (e == null) {
            e = new HashMap<String, Float>();
        }
        if (e instanceof List) {
            return ((List<String>)e).stream().collect(Collectors.toMap(s -> s, s -> 1.0f, throwingMerger(),
            CopyOnWriteMap.Tree::new));
        } else if (e instanceof Map) {
            return ((Map<String, Float>)e).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue, throwingMerger(),
            CopyOnWriteMap.Tree::new));
        }
        throw new IllegalArgumentException("Don't know how to convert '" + e + "'' to category map.");
    }

    @DataBoundConstructor
    public ThrottleJobProperty(
            Integer maxConcurrentPerNode,
            Integer maxConcurrentTotal,
            List<String> categories,
            boolean throttleEnabled,
            String throttleOption,
            boolean limitOneJobWithMatchingParams,
            String paramsToUseForLimit,
            @CheckForNull ThrottleMatrixProjectOptions matrixOptions) {
        this.maxConcurrentPerNode = maxConcurrentPerNode;
        this.maxConcurrentTotal = maxConcurrentTotal;
        this.categories = categories == null ? new CopyOnWriteArrayList<>() : new CopyOnWriteArrayList<>(categories);
        this.throttleEnabled = throttleEnabled;
        this.throttleOption = throttleOption;
        this.limitOneJobWithMatchingParams = limitOneJobWithMatchingParams;
        this.matrixOptions = matrixOptions;
        this.paramsToUseForLimit = paramsToUseForLimit;
        this.paramsToCompare = parseParamsToUseForLimit(this.paramsToUseForLimit);
    }

    /**
     * Migrates deprecated/obsolete data.
     *
     * @return Migrated version of the config
     */
    public Object readResolve() {
        if (configVersion == null) {
            configVersion = 0L;
        }
        if (categories == null) {
            categories = new CopyOnWriteArrayList<>();
        }
        if (category != null) {
            categories.add(category);
            category = null;
        }

        if (configVersion < 1 && throttleOption == null) {
            if (categories.isEmpty()) {
                throttleOption = "project";
            } else {
                throttleOption = "category";
                maxConcurrentPerNode = 0;
                maxConcurrentTotal = 0;
            }
        }
        configVersion = 1L;

        // Handle the throttleConfiguration in custom builds (not released)
        if (throttleConfiguration && matrixOptions == null) {
            matrixOptions = new ThrottleMatrixProjectOptions(false, true);
        }

        return this;
    }

    @Override
    protected void setOwner(Job<?, ?> owner) {
        super.setOwner(owner);
        if (throttleEnabled && categories != null) {
            DescriptorImpl descriptor = (DescriptorImpl) getDescriptor();
            synchronized (descriptor.propertiesByCategoryLock) {
                for (String c : categories) {
                    Map<ThrottleJobProperty, Void> properties =
                            descriptor.propertiesByCategory.computeIfAbsent(c, k -> new WeakHashMap<>());
                    properties.put(this, null);
                }
            }
        }
    }

    public boolean getThrottleEnabled() {
        return throttleEnabled;
    }

    public boolean isLimitOneJobWithMatchingParams() {
        return limitOneJobWithMatchingParams;
    }

    public String getThrottleOption() {
        return throttleOption;
    }

    public List<String> getCategories() {
        return categories;
    }

    public Map<String, Float> getUtilizations() {
        return categories.stream().collect(Collectors.toMap(k -> k, k -> 1.0f));
    }
    
    public Integer getMaxConcurrentPerNode() {
        if (maxConcurrentPerNode == null) {
            maxConcurrentPerNode = 0;
        }

        return maxConcurrentPerNode;
    }

    public Integer getMaxConcurrentTotal() {
        if (maxConcurrentTotal == null) {
            maxConcurrentTotal = 0;
        }

        return maxConcurrentTotal;
    }

    public String getParamsToUseForLimit() {
        return paramsToUseForLimit;
    }

    @CheckForNull
    public ThrottleMatrixProjectOptions getMatrixOptions() {
        return matrixOptions;
    }

    /**
     * Check if the build throttles {@link MatrixProject}s.
     * @return {@code true} if {@link MatrixProject}s should be throttled
     * @since 1.8.3
     */
    public boolean isThrottleMatrixBuilds() {
        return matrixOptions != null
                ? matrixOptions.isThrottleMatrixBuilds()
                : ThrottleMatrixProjectOptions.DEFAULT.isThrottleMatrixBuilds();
    }

    /**
     * Check if the build throttles {@link MatrixConfiguration}s.
     * @return {@code true} if {@link MatrixRun}s should be throttled
     * @since 1.8.3
     */
    public boolean isThrottleMatrixConfigurations() {
        return matrixOptions != null
                ? matrixOptions.isThrottleMatrixConfigurations()
                : ThrottleMatrixProjectOptions.DEFAULT.isThrottleMatrixConfigurations();
    }

    public List<String> getParamsToCompare() {
        if (paramsToCompare == null) {
            paramsToCompare = parseParamsToUseForLimit(paramsToUseForLimit);
        }
        return paramsToCompare;
    }

    /**
     * Compute the parameters to use for the comparison when checking when another build with the
     * same parameters is running on a node.
     *
     * @param paramsToUseForLimit A user-provided list of parameters, separated either by commas or
     *     whitespace.
     * @return A parsed representation of the user-provided list of parameters.
     */
    private static List<String> parseParamsToUseForLimit(String paramsToUseForLimit) {
        if (paramsToUseForLimit != null) {
            if (!paramsToUseForLimit.isEmpty()) {
                String[] split = ArrayUtils.nullToEmpty(paramsToUseForLimit.split(PARAMS_LIMIT_SEPARATOR));
                List<String> result = new ArrayList<>(Arrays.asList(split));
                result.removeAll(Collections.singletonList(""));
                return result;
            } else {
                return new ArrayList<>();
            }
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Get the list of categories for a given run by flow node, if that run/flow node combination is recorded for one or more categories.
     *
     * @param run the run
     * @return a map (possibly empty) from {@link FlowNode#getId} to a list of category names (possibly empty)
     */
    @NonNull
    static Map<String, Map<String, Float>> getCategoriesForRunByFlowNode(@NonNull Run<?, ?> run, DescriptorImpl descriptor) {
        //LOGGER.log(Level.INFO, "getCategoriesForRunByFlowNode for run {0}", run.getDisplayName());
        Map<String, Map<String, Float>> categoriesByNode = new HashMap<>();

        if (descriptor == null) {
            descriptor = fetchDescriptor();
        }
        for (ThrottleCategory cat : descriptor.getCategories()) {
            Map<String, Map<String, FlowEntry>> runs = descriptor.getThrottledPipelinesForCategory(cat.getCategoryName());
            //LOGGER.log(Level.INFO, "runs count for category {0}: {1}", new Object[] { cat.getCategoryName(), runs.size() });
            //for (Map.Entry<String, Map<String, FlowEntry>> e : runs.entrySet()) {
            //LOGGER.log(Level.INFO, "{0}: {1}", new Object[] { e.getKey(), e.getValue() });
            //}
            Map<String, FlowEntry> theRun = runs.get(run.getExternalizableId());
            if (theRun != null) {
                Collection<FlowEntry> nodeIds = theRun.values();
            if (nodeIds != null) {
                    for (FlowEntry nodeId : nodeIds) {
                        Map<String, Float> categories = categoriesByNode.computeIfAbsent(nodeId.flowId, k -> new HashMap<>());
                        categories.put(cat.getCategoryName(), nodeId.amount);
                }
            }
            } else {
                //LOGGER.log(Level.WARNING, "Error getting flow entries for pipeline run {0} and category {1}", new Object[] {run.getDisplayName(), cat.getCategoryName()});
        }
        }
        return categoriesByNode;
    }

    /**
     * Get all {@link Queue.Task}s with {@link ThrottleJobProperty}s attached to them.
     *
     * @param category a non-null string, the category name.
     * @return A list of {@link Queue.Task}s with {@link ThrottleJobProperty} attached.
     */
    static List<Queue.Task> getCategoryTasks(@NonNull String category, DescriptorImpl descriptor) {
        assert !StringUtils.isEmpty(category);
        List<Queue.Task> categoryTasks = new ArrayList<>();
        Collection<ThrottleJobProperty> properties;
        synchronized (descriptor.propertiesByCategoryLock) {
            Map<ThrottleJobProperty, Void> _properties = descriptor.propertiesByCategory.get(category);
            properties = _properties != null ? new ArrayList<>(_properties.keySet()) : Collections.emptySet();
        }
        for (ThrottleJobProperty t : properties) {
            if (t.getThrottleEnabled()) {
                if (t.getCategories() != null && t.getCategories().contains(category)) {
                    Job<?, ?> p = t.owner;
                    if (
                    /*is a task*/ p instanceof Queue.Task
                            && /* not deleted */ getItem(p.getParent(), p.getName()) == p
                            &&
                            /* has not since been reconfigured */ p.getProperty(ThrottleJobProperty.class) == t) {
                        categoryTasks.add((Queue.Task) p);
                        if (p instanceof MatrixProject && t.isThrottleMatrixConfigurations()) {
                            categoryTasks.addAll(((MatrixProject) p).getActiveConfigurations());
                        }
                    }
                }
            }
        }

        return categoryTasks;
    }

    /**
     * Gets a map of IDs for {@link Run}s to a list of {@link FlowNode}s currently running for a given category. Removes any
     * no longer valid run/flow node combinations from the internal tracking for that category, due to the run not being
     * found, the run not being a {@link FlowExecutionOwner.Executable}, the run no longer building, etc
     *
     * @param category The category name to look for.
     * @return a map of IDs for {@link Run}s to lists of {@link FlowNode}s for this category, if any. May be empty.
     */
    @NonNull
    static Map<String,List<FlowNode>> getThrottledPipelineRunsForCategory(@NonNull String category, @CheckForNull DescriptorImpl descriptor) {
        Map<String, List<FlowNode>> throttledPipelines = new TreeMap<>();

        if (descriptor == null) {
            descriptor = fetchDescriptor();
        }
        for (Map.Entry<String,Map<String, FlowEntry>> currentPipeline : descriptor.getThrottledPipelinesForCategory(category).entrySet()) {
            Run<?, ?> flowNodeRun = Run.fromExternalizableId(currentPipeline.getKey());
            List<FlowNode> flowNodes = new ArrayList<>();

            if (flowNodeRun == null
                    || !(flowNodeRun instanceof FlowExecutionOwner.Executable)
                    || !flowNodeRun.isBuilding()) {
                descriptor.removeAllFromPipelineRunForCategory(currentPipeline.getKey(), category, null);
            } else {
                FlowExecutionOwner executionOwner =
                        ((FlowExecutionOwner.Executable) flowNodeRun).asFlowExecutionOwner();
                if (executionOwner != null) {
                    FlowExecution execution = executionOwner.getOrNull();
                    if (execution == null) {
                        descriptor.removeAllFromPipelineRunForCategory(currentPipeline.getKey(), category, null);
                    } else {
                        for (String flowNodeId : currentPipeline.getValue().keySet()) {
                            try {
                                FlowNode node = execution.getNode(flowNodeId);
                                if (node != null) {
                                    flowNodes.add(node);
                                } else {
                                    descriptor.removeThrottledPipelineForCategory(
                                            currentPipeline.getKey(), flowNodeId, category, null);
                                }
                            } catch (IOException e) {
                                // do nothing
                            }
                        }
                    }
                }
            }
            if (!flowNodes.isEmpty()) {
                throttledPipelines.put(currentPipeline.getKey(), flowNodes);
            }
        }

        return throttledPipelines;
    }

    private static Item getItem(ItemGroup<?> group, String name) {
        if (group instanceof Jenkins) {
            return ((Jenkins) group).getItemMap().get(name);
        } else {
            return group.getItem(name);
        }
    }

    public static DescriptorImpl fetchDescriptor() {
        return Jenkins.get().getDescriptorByType(DescriptorImpl.class);
    }

    @Extension
    @Symbol("throttleJobProperty")
    public static final class DescriptorImpl extends JobPropertyDescriptor {
        private static final Logger LOGGER = Logger.getLogger(DescriptorImpl.class.getName());

        private List<ThrottleCategory> categories;

        @Deprecated private Map<String,Map<String,List<String>>> throttledPipelinesByCategory;

        private Map<String,Map<String,Map<String, FlowEntry>>> throttledPipelinesByCategoryFe;
        
        /** Map from category names, to properties including that category. */
        private transient Map<String, Map<ThrottleJobProperty, Void>> propertiesByCategory = new HashMap<>();
        /** A sync object for {@link #propertiesByCategory} */
        private final transient Object propertiesByCategoryLock = new Object();

        public DescriptorImpl() {
            super(ThrottleJobProperty.class);
            synchronized (propertiesByCategoryLock) {
                load();
                // Explictly handle the persisted data from the version 1.8.1
                if (propertiesByCategory == null) {
                    propertiesByCategory = new HashMap<>();
                }
                if (throttledPipelinesByCategoryFe == null) {
                    throttledPipelinesByCategoryFe = new HashMap<>();
                } else {
                    throttledPipelinesByCategoryFe.clear();
                }
                if (throttledPipelinesByCategory != null && !throttledPipelinesByCategory.isEmpty()) {
                    throttledPipelinesByCategory = new HashMap<>();
                } else if (throttledPipelinesByCategory != null) {
                    throttledPipelinesByCategory.clear();
                }
                if (!propertiesByCategory.isEmpty()) {
                    propertiesByCategory.clear();
                    save(); // Save the configuration to remove obsolete data
                }
            }
        }

        @Override
        public String getDisplayName() {
            return "Throttle Concurrent Builds";
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean isApplicable(Class<? extends Job> jobType) {
            return Job.class.isAssignableFrom(jobType) && Queue.Task.class.isAssignableFrom(jobType);
        }

        public boolean isMatrixProject(Job<?, ?> job) {
            return job instanceof MatrixProject;
        }

        @Override
        public boolean configure(StaplerRequest req, JSONObject formData) throws FormException {
            if (!formData.has("categories")) {
                this.categories = null;
            }

            req.bindJSON(this, formData);
            save();
            return true;
        }

        @SuppressWarnings({"lgtm[jenkins/csrf]", "lgtm[jenkins/no-permission-check]"})
        public FormValidation doCheckCategoryName(@QueryParameter String value) {
            if (Util.fixEmptyAndTrim(value) == null) {
                return FormValidation.error("Empty category names are not allowed.");
            } else {
                return FormValidation.ok();
            }
        }

        @SuppressWarnings({"lgtm[jenkins/csrf]", "lgtm[jenkins/no-permission-check]"})
        public FormValidation doCheckMaxConcurrentPerNode(@QueryParameter String value) {
            return checkNullOrInt(value);
        }

        private FormValidation checkNullOrInt(String value) {
            // Allow nulls - we'll just translate those to 0s.
            if (Util.fixEmptyAndTrim(value) != null) {
                return FormValidation.validateNonNegativeInteger(value);
            } else {
                return FormValidation.ok();
            }
        }

        @SuppressWarnings({"lgtm[jenkins/csrf]", "lgtm[jenkins/no-permission-check]"})
        public FormValidation doCheckMaxConcurrentTotal(@QueryParameter String value) {
            return checkNullOrInt(value);
        }

        public ThrottleCategory getCategoryByName(String categoryName) {
            ThrottleCategory category = null;

            if (categories != null) {
                for (ThrottleCategory tc : categories) {
                    if (tc.getCategoryName().equals(categoryName)) {
                        category = tc;
                    }
                }
            }

            return category;
        }

        public void setCategories(List<ThrottleCategory> categories) {
            this.categories = new CopyOnWriteArrayList<>(categories);
        }

        public List<ThrottleCategory> getCategories() {
            if (categories == null) {
                categories = new CopyOnWriteArrayList<>();
            }

            return categories;
        }

        @SuppressWarnings("lgtm[jenkins/csrf]")
        public ListBoxModel doFillCategoryItems(@AncestorInPath Item item) {
            if (item != null) {
                item.checkPermission(Item.CONFIGURE);
            } else {
                Jenkins.get().checkPermission(Jenkins.ADMINISTER);
            }

            ListBoxModel m = new ListBoxModel();

            m.add("(none)", "");

            for (ThrottleCategory tc : getCategories()) {
                m.add(tc.getCategoryName());
            }

            return m;
        }

        @Override
        public void load() {
            super.load();
            initThrottledPipelines();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "load: {0}", throttledPipelinesByCategoryFe);
        }
        }

        private synchronized void initThrottledPipelines() {
            if (throttledPipelinesByCategoryFe == null) {
                throttledPipelinesByCategoryFe = new TreeMap<>();
            } else if (throttledPipelinesByCategoryFe.entrySet().stream()
                    .anyMatch(e -> !(e.getValue() instanceof CopyOnWriteMap.Tree))) {
                // if any of the nested maps are not copy-on-write tree maps, convert the whole data
                // structure.
                LOGGER.log(Level.INFO, "Migrating throttled pipelines by category to copy-on-write data structures.");
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.log(Level.FINE, "Original values: {0}", throttledPipelinesByCategoryFe);
                }
                // For consistency, the type of map returned below should match that of the
                // initThrottledPipelines method.
                throttledPipelinesByCategoryFe =
                        throttledPipelinesByCategoryFe.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                Map.Entry::getKey,
                                DescriptorImpl::convertValueToCopyOnWriteDataStructures,
                                throwingMerger(),
                                TreeMap::new));

                LOGGER.log(
                        Level.INFO,
                        "Finished migrating throttled pipelines by category to copy-on-write data structures. Immediately persisting migrated state.");
                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.log(Level.FINE, "New values: {0}", throttledPipelinesByCategoryFe);
                }

                // persist state, now that the data structures have been converted.
                save();

                LOGGER.log(Level.INFO, "Migrated state persisted successfully.");
            }
        }

        /**
         * Converts a map entry's value of a map of string list to use copy-on-write data structures
         * for both the map and contained lists.
         *
         * @param original map entry of the map of keys to array lists
         * @return the same map entry's value as a copy-on-write tree map with copy-on-write
         *     array lists as values.
         */
        private static Map<String, Map<String, FlowEntry>> convertValueToCopyOnWriteDataStructures(
                Map.Entry<String, Map<String, Map<String, FlowEntry>>> original) {
            return original.getValue().entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                                    e -> convertFlowObjectToMap(e),//new CopyOnWriteMap.Tree<>(e.getValue(), CaseInsensitiveComparator.INSTANCE),
                            throwingMerger(),
                            CopyOnWriteMap.Tree::new));
        }

        @SuppressWarnings("unchecked")
        private static Map<String, FlowEntry> convertFlowObjectToMap(Map.Entry<String, Map<String, FlowEntry>> e) {
            if (e.getValue() instanceof List) {
                return ((List<String>)e.getValue()).stream().collect(Collectors.toMap(s -> s, s -> new FlowEntry(s, e.getKey(), 1.0f), throwingMerger(),
                CopyOnWriteMap.Tree::new));
            } else if (e.getValue() instanceof Map) {
                return ((Map<String, FlowEntry>)e.getValue()).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue, throwingMerger(),
                CopyOnWriteMap.Tree::new));
        }
            throw new IllegalArgumentException("Don't know how to convert '" + e.getValue() + "'' to category flow entry map.");
        }

        @Override
        public void save() {
            super.save();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "save: {0}", throttledPipelinesByCategoryFe);
        }
        }

        @NonNull
        public Map<String,Map<String, FlowEntry>> getThrottledPipelinesForCategory(@NonNull String category) {
            synchronized (rwLock.readLock()) {
            return internalGetThrottledPipelinesForCategory(category);
        }
        }

        @NonNull
        private Map<String,Map<String, FlowEntry>> internalGetThrottledPipelinesForCategory(@NonNull String category) {
            if (getCategoryByName(category) != null) {
                if (throttledPipelinesByCategoryFe.containsKey(category)) {
                    return throttledPipelinesByCategoryFe.get(category);
                }
            }
            return new CopyOnWriteMap.Tree<>();
        }
        private class CatPipelineQueueItem {
            String runId;
            String fnId;
            FlowEntry fe;
            String category;
            TaskListener listener;
        }
        private transient ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private transient java.util.concurrent.BlockingQueue<CatPipelineQueueItem> addedCatPipelines = new LinkedBlockingQueue<>();
        private transient java.util.concurrent.BlockingQueue<CatPipelineQueueItem> removedCatPipelines = new LinkedBlockingQueue<>();
        public void addThrottledPipelineForCategoryDeferred(@NonNull String runId, @NonNull FlowEntry fe, TaskListener listener) {
            if (addedCatPipelines.size() > 2000) {
                LOGGER.log(Level.SEVERE, "addThrottledPipelineForCategoryDeferred {0}, {1} addedCatPipelines overflow, Queue maintenance cannot keep up?", new Object[]{runId, fe});
                addedCatPipelines.clear();
            }
            CatPipelineQueueItem cpi = new CatPipelineQueueItem();
            cpi.runId = runId;
            cpi.fe = fe;
            cpi.category = null;
            cpi.listener = listener;
            addedCatPipelines.add(cpi);
        }
        public void addThrottledPipelineForCategory(@NonNull String runId, @NonNull FlowEntry fe, TaskListener listener) {
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.log(Level.FINEST, "addThrottledPipelineForCategory {0}, {1}", new Object[]{runId, fe});
            }
            synchronized (rwLock.writeLock()) {
                if (getCategoryByName(fe.category) == null) {
                if (listener != null) {
                        listener.getLogger().println(Messages.ThrottleJobProperty_DescriptorImpl_NoSuchCategory(fe.category));
                }
            } else {
                    Map<String, Map<String, FlowEntry>> currentPipelines = internalGetThrottledPipelinesForCategory(fe.category);

                    Map<String, FlowEntry> flowNodes = currentPipelines.get(runId);
                if (flowNodes == null) {
                        flowNodes = new CopyOnWriteMap.Tree<>();
                }
                    flowNodes.put(fe.flowId, fe);
                currentPipelines.put(runId, flowNodes);
                    throttledPipelinesByCategoryFe.put(fe.category, currentPipelines);
            }
        }
        }

        public void removeThrottledPipelineForCategoryDeferred(@NonNull String runId, @NonNull String flowNodeId, @NonNull String category, TaskListener listener) {
            if (removedCatPipelines.size() > 2000) {
                LOGGER.log(Level.SEVERE, "addThrottledPipelineForCategoryDeferred {0}, {1} removedCatPipelines overflow, Queue maintenance cannot keep up?", new Object[]{runId, flowNodeId});
                removedCatPipelines.clear();
            }
            CatPipelineQueueItem cpi = new CatPipelineQueueItem();
            cpi.runId = runId;
            cpi.fnId = flowNodeId;
            cpi.category = category;
            cpi.listener = listener;
            removedCatPipelines.add(cpi);
        }

        public void processQueues() {
            ArrayList<CatPipelineQueueItem> drain = new ArrayList<>();
            addedCatPipelines.drainTo(drain);
            for (CatPipelineQueueItem c : drain) {
                addThrottledPipelineForCategory(c.runId, c.fe, c.listener);
            }
            drain.clear();
            removedCatPipelines.drainTo(drain);
            for (CatPipelineQueueItem c : drain) {
                removeThrottledPipelineForCategory(c.runId, c.fnId, c.category, c.listener);
            }
        }

        public void removeThrottledPipelineForCategory(@NonNull String runId,
                                                                    @NonNull String flowNodeId,
                                                                    @NonNull String category,
                                                                    TaskListener listener) {
            synchronized (rwLock.writeLock()) {
            if (getCategoryByName(category) == null) {
                if (listener != null) {
                    listener.getLogger().println(Messages.ThrottleJobProperty_DescriptorImpl_NoSuchCategory(category));
                }
            } else {
                    Map<String, Map<String, FlowEntry>> currentPipelines = internalGetThrottledPipelinesForCategory(category);

                if (!currentPipelines.isEmpty()) {
                        Map<String, FlowEntry> flowNodes = currentPipelines.get(runId);
                    if (flowNodes != null) {
                        flowNodes.remove(flowNodeId);
                    }
                    if (flowNodes != null && !flowNodes.isEmpty()) {
                        currentPipelines.put(runId, flowNodes);
                    } else {
                        currentPipelines.remove(runId);
                    }
                }

                if (currentPipelines.isEmpty()) {
                        throttledPipelinesByCategoryFe.remove(category);
                } else {
                        throttledPipelinesByCategoryFe.put(category, currentPipelines);
                }
            }
        }
        }

        public void removeAllFromPipelineRunForCategory(@NonNull String runId,
                                                                     @NonNull String category,
                                                                     TaskListener listener) {
            synchronized (rwLock.writeLock()) {
            if (getCategoryByName(category) == null) {
                if (listener != null) {
                    listener.getLogger().println(Messages.ThrottleJobProperty_DescriptorImpl_NoSuchCategory(category));
                }
            } else {
                    Map<String, Map<String, FlowEntry>> currentPipelines = internalGetThrottledPipelinesForCategory(category);

                if (!currentPipelines.isEmpty()) {
                    currentPipelines.remove(runId);
                }
                if (currentPipelines.isEmpty()) {
                        throttledPipelinesByCategoryFe.remove(category);
                } else {
                        throttledPipelinesByCategoryFe.put(category, currentPipelines);
                }
            }
        }
    }
    }

    public static float toFloat(Object o, Float def) {
        if (o instanceof Float) {
            return (Float)o;
        }
        if (o instanceof Integer) {
            return (Integer)o;
        }
        if (o instanceof Double) {
            return ((Double)o).floatValue();
        }
        if (o instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal)o).floatValue();
        }
        if (def == null) {
            throw new IllegalArgumentException("Don't know how to convert " + o + " to float.");
        }
        return def;
    }

    public static float toFloat(Object o) {
        return toFloat(o, null);
    }

    public static final class ThrottleCategory extends AbstractDescribableImpl<ThrottleCategory> {
        private Integer maxConcurrentPerNode;
        private Integer maxConcurrentTotal;
        private String categoryName;
        private List<NodeLabeledPair> nodeLabeledPairs;

        @DataBoundConstructor
        public ThrottleCategory(
                String categoryName,
                Integer maxConcurrentPerNode,
                Integer maxConcurrentTotal,
                List<NodeLabeledPair> nodeLabeledPairs) {
            this.maxConcurrentPerNode = maxConcurrentPerNode;
            this.maxConcurrentTotal = maxConcurrentTotal;
            this.categoryName = categoryName;
            this.nodeLabeledPairs = nodeLabeledPairs == null ? new ArrayList<>() : nodeLabeledPairs;
        }

        public Integer getMaxConcurrentPerNode() {
            if (maxConcurrentPerNode == null) {
                maxConcurrentPerNode = 0;
            }

            return maxConcurrentPerNode;
        }

        public Integer getMaxConcurrentTotal() {
            if (maxConcurrentTotal == null) {
                maxConcurrentTotal = 0;
            }

            return maxConcurrentTotal;
        }

        public String getCategoryName() {
            return categoryName;
        }

        public List<NodeLabeledPair> getNodeLabeledPairs() {
            if (nodeLabeledPairs == null) {
                nodeLabeledPairs = new ArrayList<>();
            }

            return nodeLabeledPairs;
        }

        @Extension
        public static class DescriptorImpl extends Descriptor<ThrottleCategory> {
            @Override
            public String getDisplayName() {
                return "";
            }
        }
    }

    /**
     * @author marco.miller@ericsson.com
     */
    public static final class NodeLabeledPair extends AbstractDescribableImpl<NodeLabeledPair> {
        private String throttledNodeLabel;
        private Integer maxConcurrentPerNodeLabeled;

        @DataBoundConstructor
        public NodeLabeledPair(String throttledNodeLabel, Integer maxConcurrentPerNodeLabeled) {
            this.throttledNodeLabel = throttledNodeLabel == null ? "" : throttledNodeLabel;
            this.maxConcurrentPerNodeLabeled = maxConcurrentPerNodeLabeled;
        }

        public String getThrottledNodeLabel() {
            if (throttledNodeLabel == null) {
                throttledNodeLabel = "";
            }
            return throttledNodeLabel;
        }

        public Integer getMaxConcurrentPerNodeLabeled() {
            if (maxConcurrentPerNodeLabeled == null) {
                maxConcurrentPerNodeLabeled = 0;
            }
            return maxConcurrentPerNodeLabeled;
        }

        @Extension
        public static class DescriptorImpl extends Descriptor<NodeLabeledPair> {
            @Override
            public String getDisplayName() {
                return "";
            }
        }
    }
    private static final Logger LOGGER = Logger.getLogger(ThrottleJobProperty.class.getName());
}
