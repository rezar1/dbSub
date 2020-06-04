package com.Rezar.dbSub.utils;

import java.util.Vector;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月27日 下午7:23:26
 * @Desc 些年若许,不负芳华.
 *
 */

public class SelfObservable {

	private boolean changed = false;
	private Vector<SelfObserver> obs;
	private volatile Object[] arrLocal;

	public SelfObservable() {
		obs = new Vector<>();
	}

	public synchronized void addObserver(SelfObserver o) {
		if (o == null)
			throw new NullPointerException();
		if (!obs.contains(o)) {
			this.arrLocal = null;
			obs.addElement(o);
		}
	}

	public synchronized void deleteObserver(SelfObserver o) {
		obs.removeElement(o);
		arrLocal = null;
	}

	public void notifyObservers() {
		notifyObservers(null);
	}

	public void notifyObservers(Object arg) {
		synchronized (this) {
			if (!changed)
				return;
			if (arrLocal == null) {
				arrLocal = obs.toArray();
			}
			clearChanged();
		}
		for (int i = arrLocal.length - 1; i >= 0; i--) {
			((SelfObserver) arrLocal[i]).update(this, arg);
		}
	}

	public synchronized boolean containObservers(SelfObserver observer) {
		return this.obs.contains(observer);
	}

	public synchronized void deleteObservers() {
		obs.removeAllElements();
		arrLocal = null;
	}

	protected synchronized void setChanged() {
		changed = true;
	}

	protected synchronized void clearChanged() {
		changed = false;
	}

	public synchronized boolean hasChanged() {
		return changed;
	}

	public synchronized int countObservers() {
		return obs.size();
	}
}
