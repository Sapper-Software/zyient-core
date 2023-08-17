package ai.sapper.cdc.intake.flow;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class TaskGroupResponse<T> {
	private String namespace;
	private String name;
	private String instanceId;
	private String runId;
	private String dataSource;
	private int recordCount;
	private int errorCount;
	private long startTime;
	private long endTime;
	private List<TaskExecutionRecord<T>> results;
	private Throwable error;
	
	public TaskGroupResponse() {
		// TODO Auto-generated constructor stub
	}
	
	public boolean hasError() {
		return (error != null);
	}
}
