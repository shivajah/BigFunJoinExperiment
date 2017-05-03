/*
 * Copyright by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package structure;

import datatype.IArgument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author pouria
 */
public class Query {

    private List<String> segments;
    private List<IArgument> args;

    public Query(List<String> segments) {
        this.segments = new ArrayList<String>();
        for (String s : segments) {
            this.segments.add(s);
        }
        this.args = new ArrayList<IArgument>();
    }

    public String aqlPrint(String dataverse) {
        Iterator<String> segIt = segments.iterator();
        Iterator<IArgument> argIt = args.iterator();

        StringBuilder st = new StringBuilder("use dataverse " + dataverse + ";\n");
        while (segIt.hasNext()) {
            st.append(segIt.next()).append(" ");
            if (argIt.hasNext()) {
                st.append(argIt.next().admPrint()).append(" ");
            }
        }
        if (argIt.hasNext()) {
            st.append(" ").append(argIt.next().admPrint());
        }
        System.out.println("query: " + st.toString());
        return st.toString();
    }
    public String aqlPrint(String dataverse,double joinMemory,double groupMemory,double framesize) {
        Iterator<String> segIt = segments.iterator();
        Iterator<IArgument> argIt = args.iterator();

        StringBuilder st = new StringBuilder("use dataverse " + dataverse + ";\n");
        if(joinMemory > 0){
            st.append("set "+'"'+"compiler.joinmemory"+'"'+' '+'"'+joinMemory+"MB"+'"'+"\n");
        }
        if (groupMemory > 0){
            st.append("set "+'"'+"compiler.groupmemory"+'"'+' '+'"'+groupMemory+"MB"+'"'+"\n");
        }
        if(framesize > 0) {
            st.append("set "+'"'+"compiler.framesize"+'"'+' '+'"'+framesize+"MB"+'"'+"\n");
        }
        while (segIt.hasNext()) {
            st.append(segIt.next()).append(" ");
            if (argIt.hasNext()) {
                st.append(argIt.next().admPrint()).append(" ");
            }
        }
        if (argIt.hasNext()) {
            st.append(" ").append(argIt.next().admPrint());
        }
       // System.out.println("query: " + st.toString());
        return st.toString();
    }

    public void reset(List<IArgument> a) {
        args.clear();
        for (IArgument arg : a) {
            args.add(arg);
        }
    }

    public List<IArgument> getArguments() {
        return this.args;
    }

}