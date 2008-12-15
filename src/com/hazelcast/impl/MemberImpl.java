/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
 
package com.hazelcast.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;

public class MemberImpl implements Member {

	private boolean thisMember = false;

	private Address address;

	private MemberImpl nextMember = null;

	private boolean ready = false;

	public MemberImpl(Address address, boolean thisMember) {
		super();
		this.thisMember = thisMember;
		this.address = address;
	}

	public MemberImpl(Address address) {
		this.address = address;
	}

	public boolean isReady() {
		return ready;
	}

	public void setReady(boolean ready) {
		this.ready = ready;
	}

	public Address getAddress() {
		return address;
	}

	public int getPort() {
		return address.getPort();
	}

	public InetAddress getInetAddress() {
		try {
			return address.getInetAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}

	public MemberImpl getNextMember() {
		return nextMember;
	}

	public void setNextMember(MemberImpl nextMember) {
		this.nextMember = nextMember;
	}

	public boolean localMember() {
		return thisMember;
	}

	public void setThisMember(boolean thisMember) {
		this.thisMember = thisMember;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Member [");
		sb.append(address.getHost());
		sb.append(":");
		sb.append(address.getPort());
		sb.append("] ");
		if (thisMember) {
			sb.append("this ");
		}
		if (Node.DEBUG && Node.get().getMasterAddress().equals(address)) {
			sb.append("* ");
		}
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + ((address == null) ? 0 : address.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final MemberImpl other = (MemberImpl) obj;
		if (address == null) {
			if (other.address != null)
				return false;
		} else if (!address.equals(other.address))
			return false;
		return true;
	}

}
