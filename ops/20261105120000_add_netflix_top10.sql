-- Additive only: two new tables, no changes to existing tables or columns.
CREATE TABLE "NetflixTop10Entry" (
    "id" SERIAL NOT NULL,
    "scope" TEXT NOT NULL,
    "week" DATE NOT NULL,
    "category" TEXT NOT NULL,
    "rank" INTEGER NOT NULL,
    "weeksInTop10" INTEGER NOT NULL,
    "showTitle" TEXT NOT NULL,
    "seasonTitle" TEXT,
    "mediaType" TEXT NOT NULL,
    "tmdbId" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "NetflixTop10Entry_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "NetflixTitleMatch" (
    "id" SERIAL NOT NULL,
    "titleKey" TEXT NOT NULL,
    "mediaType" TEXT NOT NULL,
    "tmdbId" INTEGER,
    "status" TEXT NOT NULL,
    "matchedTitle" TEXT,
    "checkedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "NetflixTitleMatch_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "NetflixTop10Entry_scope_week_category_rank_key" ON "NetflixTop10Entry"("scope", "week", "category", "rank");
CREATE INDEX "NetflixTop10Entry_scope_category_week_idx" ON "NetflixTop10Entry"("scope", "category", "week");
CREATE INDEX "NetflixTop10Entry_tmdbId_mediaType_idx" ON "NetflixTop10Entry"("tmdbId", "mediaType");
CREATE UNIQUE INDEX "NetflixTitleMatch_titleKey_mediaType_key" ON "NetflixTitleMatch"("titleKey", "mediaType");
