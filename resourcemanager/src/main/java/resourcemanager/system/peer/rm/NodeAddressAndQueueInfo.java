/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import se.sics.kompics.address.Address;

/**
 *
 * @author johsn
 */
public class NodeAddressAndQueueInfo
{
    private int queue;

    public NodeAddressAndQueueInfo(int queue, Address source) {
        this.queue = queue;
        this.source = source;
    }

    public int getQueue() {
        return queue;
    }

    public void setQueue(int queue) {
        this.queue = queue;
    }

    public Address getSource() {
        return source;
    }

    public void setSource(Address source) {
        this.source = source;
    }
    private Address source;
}
