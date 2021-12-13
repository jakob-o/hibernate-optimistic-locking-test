/*
 * Copyright 2014 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package org.hibernate.bugs;

import org.hibernate.Session;
import org.hibernate.StaleObjectStateException;
import org.hibernate.Transaction;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.OptimisticLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This template demonstrates how to develop a test case for Hibernate ORM, using its built-in unit test framework.
 * Although ORMStandaloneTestCase is perfectly acceptable as a reproducer, usage of this class is much preferred.
 * Since we nearly always include a regression test with bug fixes, providing your reproducer using this method
 * simplifies the process.
 *
 * What's even better?  Fork hibernate-orm itself, add your test case directly to a module's unit tests, then
 * submit it as a PR!
 */
public class OptimisticLockingTestCase extends BaseCoreFunctionalTestCase {

	private static final Logger log = LoggerFactory.getLogger(OptimisticLockingTestCase.class);

	// Add your entities here.
	@Override
	protected Class[] getAnnotatedClasses() {
		return new Class[] {
				VersionedEntity.class
		};
	}

	// Add in any settings that are specific to your test.  See resources/hibernate.properties for the defaults.
	@Override
	protected void configure(Configuration configuration) {
		super.configure( configuration );

		configuration.setProperty( AvailableSettings.SHOW_SQL, Boolean.TRUE.toString() );
		configuration.setProperty( AvailableSettings.FORMAT_SQL, Boolean.TRUE.toString() );
	}

	static class ThreadedSaveExecution implements Runnable {

		private final long entityId;
		private final Session session;
		private final CountDownLatch saveCountDownLatch;
		private final List<Exception> exceptions;

		ThreadedSaveExecution(long entityId, Session session, CountDownLatch saveCountDownLatch,
				List<Exception> exceptions) {
			this.session = session;
			this.entityId = entityId;
			this.saveCountDownLatch = saveCountDownLatch;
			this.exceptions = exceptions;
		}

		@Override
		public void run() {
			Transaction tx = session.beginTransaction();
			VersionedEntity versionedEntity = session.get(VersionedEntity.class, entityId);
			versionedEntity.incVersion();
			session.save(versionedEntity);

			log.info("Saved entity {} with id {}, and version {}", versionedEntity,
					versionedEntity.getId(), versionedEntity.getVersion());

			saveCountDownLatch.countDown();
			try {
				saveCountDownLatch.await();
			} catch (InterruptedException e) {
				log.error("Await got interrupted", e);
				session.close();
				return;
			}
			try {
				tx.commit();
				log.info("Committed transaction {}", tx);
			} catch (Exception e) {
				log.info("Failed to commit transaction {}", tx, e);
				exceptions.add(e);
			} finally {
				session.close();
			}
		}
	}

	@Test
	public void hhh14973Test() throws Exception {
		Session session = openSession();
		Transaction tx = session.beginTransaction();
		VersionedEntity versionedEntity = new VersionedEntity();
		session.save(versionedEntity);
		tx.commit();
		session.close();
		long entityId = versionedEntity.getId();

		List<Exception> exceptions = new ArrayList<>();
		CountDownLatch saveCountDownLatch = new CountDownLatch(2);

		Session session1 = openSession();
		Thread t1 = new Thread(new ThreadedSaveExecution(
				entityId,
				session1,
				saveCountDownLatch,
				exceptions
		));
		Session session2 = openSession();
		Thread t2 = new Thread(new ThreadedSaveExecution(
				entityId,
				session2,
				saveCountDownLatch,
				exceptions
		));
		t1.start();
		t2.start();
		saveCountDownLatch.await();
		t1.join();
		t2.join();

		assertEquals(1, exceptions.size());
		Exception e = exceptions.get(0);
		assertTrue(e instanceof OptimisticLockException);
		Throwable cause = e.getCause();
		assertTrue("Cause actually is " + cause, cause instanceof StaleObjectStateException);
		StaleObjectStateException staleObjectStateException = (StaleObjectStateException) cause;
		assertEquals(entityId, staleObjectStateException.getIdentifier());
		assertEquals(VersionedEntity.class.getName(), staleObjectStateException.getEntityName());
	}
}
