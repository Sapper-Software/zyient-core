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

package ai.sapper.cdc.common.cache;

public class DummyNode<T> implements LinkedListNode<T> {
    private DoublyLinkedList<T> list;

    public DummyNode(DoublyLinkedList<T> list) {
        this.list = list;
    }

    @Override
    public boolean hasElement() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public T getElement() throws NullPointerException {
        throw new NullPointerException();
    }

    @Override
    public void detach() {
        return;
    }

    @Override
    public DoublyLinkedList<T> getListReference() {
        return list;
    }

    @Override
    public LinkedListNode<T> setPrev(LinkedListNode<T> next) {
        return next;
    }

    @Override
    public LinkedListNode<T> setNext(LinkedListNode<T> prev) {
        return prev;
    }

    @Override
    public LinkedListNode<T> getPrev() {
        return this;
    }

    @Override
    public LinkedListNode<T> getNext() {
        return this;
    }

    @Override
    public LinkedListNode<T> search(T value) {
        return this;
    }
}
