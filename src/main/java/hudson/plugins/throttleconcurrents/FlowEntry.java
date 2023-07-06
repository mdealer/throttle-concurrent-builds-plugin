package hudson.plugins.throttleconcurrents;

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
import hudson.util.CaseInsensitiveComparator;
import hudson.util.CopyOnWriteMap;
import hudson.util.CopyOnWriteMap.Tree;
import hudson.util.FormValidation;
import hudson.util.ListBoxModel;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Comparator;
import java.util.stream.Collectors;
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
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;
import org.kohsuke.stapler.StaplerRequest;

public class FlowEntry implements Comparable<FlowEntry>, Serializable {
    public FlowEntry(String flowId, String category, Float amount) {
        this.flowId = flowId;
        this.category = category;
        this.amount = amount;
    }
    public int compareTo(FlowEntry obj)
    {
        return flowId.compareTo(obj.flowId);
    }
    public int hashCode() {
        return flowId.hashCode();
    }
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof FlowEntry) {
            FlowEntry fe = (FlowEntry)obj;
            return flowId.equals(fe.flowId);
        }
        return false;
    }    
    public String flowId;
    public String category;
    public Float amount;
}
