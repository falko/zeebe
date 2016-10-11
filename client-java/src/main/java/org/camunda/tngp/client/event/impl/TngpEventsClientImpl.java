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
package org.camunda.tngp.client.event.impl;

import org.camunda.tngp.client.EventsClient;
import org.camunda.tngp.client.event.cmd.PollEventsCmd;
import org.camunda.tngp.client.event.impl.cmd.PollEventsCmdImpl;
import org.camunda.tngp.client.impl.ClientCmdExecutor;

public class TngpEventsClientImpl implements EventsClient
{
    protected final ClientCmdExecutor commandExecutor;

    public TngpEventsClientImpl(ClientCmdExecutor commandExecutor)
    {
        this.commandExecutor = commandExecutor;
    }

    @Override
    public PollEventsCmd poll()
    {
        return new PollEventsCmdImpl(commandExecutor);
    }

}
