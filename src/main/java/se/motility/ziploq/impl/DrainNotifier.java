/*
 * Copyright (c) 2026 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.ziploq.impl;

/**
 * A quality to be implemented by a queue or similar data structure, used for creating an efficient scheduler.
 * After activation, the drain signal will be raised after a significant number of elements have been drained
 * from the implementing queue. The definition of 'significant' is left to the implementer.
 *
 * @author M Tegling
 */
public interface DrainNotifier {

    /**
     * Requests the implementing class to run the supplied action after a significant number
     * of elements have been drained
     * @param action to run when implementing class has drained sufficiently
     */
    void requestNotification(Runnable action);

}
