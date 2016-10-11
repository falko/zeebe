/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.client.event.impl.cmd;

import org.camunda.tngp.client.event.EventsBatch;
import org.camunda.tngp.client.event.cmd.PollEventsCmd;
import org.camunda.tngp.client.event.impl.EventBatchHandler;
import org.camunda.tngp.client.impl.ClientCmdExecutor;
import org.camunda.tngp.client.impl.cmd.AbstractCmdImpl;
import org.camunda.tngp.protocol.event.PollEventsRequestWriter;
import org.camunda.tngp.util.buffer.RequestWriter;

public class PollEventsCmdImpl extends AbstractCmdImpl<EventsBatch> implements PollEventsCmd
{
    protected PollEventsRequestWriter requestWriter = new PollEventsRequestWriter();

    public PollEventsCmdImpl(ClientCmdExecutor cmdExecutor)
    {
        super(cmdExecutor, new EventBatchHandler());
    }

    @Override
    public PollEventsCmd startPosition(long position)
    {
        requestWriter.startPosition(position);
        return this;
    }

    @Override
    public PollEventsCmd maxEvents(int maxEvents)
    {
        requestWriter.maxEvents(maxEvents);
        return this;
    }

    @Override
    public PollEventsCmd topicId(int topicId)
    {
        requestWriter.topicId(topicId);
        return this;
    }

    @Override
    public RequestWriter getRequestWriter()
    {
        return requestWriter;
    }

    public void setRequestWriter(PollEventsRequestWriter requestWriter)
    {
        this.requestWriter = requestWriter;
    }

}
