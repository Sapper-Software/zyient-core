/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.decisions;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

public class EvaluationTree<T, R> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class Node<T, R> {
        private Condition<T> condition;
        private R result;
        private List<Node<T, R>> children;

        public Node<T, R> add(@NonNull Node<T, R> child) {
            if (children == null) {
                children = new ArrayList<>();
            }
            children.add(child);
            return child;
        }

        public Node<T, R> add(@NonNull Condition<T> condition,
                              R result) {
            Node<T, R> node = new Node<>();
            node.condition = condition;
            node.result = result;
            return add(node);
        }

        public R evaluate(@NonNull T data,
                          R result) throws Exception {
            if (condition.evaluate(data)) {
                result = result == null ? this.result : result;
                if (children != null) {
                    for (Node<T, R> child : children) {
                        result = child.evaluate(data, result);
                    }
                }
            } else {
                result = this.result;
            }
            return result;
        }
    }

    public List<Node<T, R>> branches = new ArrayList<>();

    public R evaluate(@NonNull T data) throws Exception {
        R result = null;
        for (Node<T, R> node : branches) {
            result = node.evaluate(data, result);
            if (result != null) break;
        }
        return result;
    }

    public Node<T, R> add(@NonNull Condition<T> condition,
                          R result) {
        Node<T, R> node = new Node<>();
        node.condition = condition;
        node.result = result;
        branches.add(node);
        return node;
    }
}
