/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.intake.flow;

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
