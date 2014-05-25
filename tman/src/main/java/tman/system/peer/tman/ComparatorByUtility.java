/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tman.system.peer.tman;

import cyclon.system.peer.cyclon.PeerDescriptor;
import java.util.Comparator;

/**
 *
 * @author johsn
 */
class ComparatorByUtility implements Comparator<PeerDescriptor>{
    private final String _type;
    
    public ComparatorByUtility(String type) {
        this._type = type;
    }

    @Override
    public int compare(PeerDescriptor o1, PeerDescriptor o2)
    {
        
        if(_type.equals("cpu"))
        {
            if(o1.getAvailableResources().getNumFreeCpus() < o2.getAvailableResources().getNumFreeCpus())
            {
                return 1;
            }
            else if(o1.getAvailableResources().getNumFreeCpus() > o2.getAvailableResources().getNumFreeCpus())
            {
                return -1;
            }
            else if(o1.getAvailableResources().getFreeMemInMbs() < o2.getAvailableResources().getFreeMemInMbs())
            {
                return 1;
            }
            else if(o1.getAvailableResources().getFreeMemInMbs() > o2.getAvailableResources().getFreeMemInMbs())
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }
        else
        {
            if(o1.getAvailableResources().getFreeMemInMbs() < o2.getAvailableResources().getFreeMemInMbs())
            {
                return 1;
            }
            else if(o1.getAvailableResources().getFreeMemInMbs() > o2.getAvailableResources().getFreeMemInMbs())
            {
                return -1;
            }
            else if(o1.getAvailableResources().getNumFreeCpus() < o2.getAvailableResources().getNumFreeCpus())
            {
                return 1;
            }
            else if(o1.getAvailableResources().getNumFreeCpus() > o2.getAvailableResources().getNumFreeCpus())
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }
    }
    
}
