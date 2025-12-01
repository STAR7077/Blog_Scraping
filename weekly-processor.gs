/**
 * 週間ランキングプロセッサークラス
 * 週間データ集計、ランキング計算、トレンド分析を処理
 */
class WeeklyProcessor {
  constructor() {
    this.weeklyData = [];
    this.rankings = {};
    this.trends = {};
  }
  
  /**
   * 週間データを処理してランキングを計算
   * @param {Array} dailyData - 日次データ
   * @return {Object} 週間ランキングデータ
   */
  processWeeklyRankings(dailyData) {
    Logger.log("週間ランキング処理を開始...");
    
    try {
      // 日次データを週間データに集計
      Logger.log("ステップ1: 日次データを集計中...");
      const weeklyAggregated = this.aggregateWeeklyData(dailyData);
      Logger.log(`${weeklyAggregated.length}件の週間レコードを集計`);
      
      // 週間ランキングを計算
      Logger.log("ステップ2: 週間ランキングを計算中...");
      const weeklyRankings = this.calculateWeeklyRankings(weeklyAggregated);
      Logger.log(`${Object.keys(weeklyRankings).length}カテゴリのランキングを計算`);
      
      // 週間レポートを生成
      Logger.log("ステップ3: 週間レポートを生成中...");
      const weeklyReports = this.generateWeeklyReports(weeklyRankings);
      Logger.log(`${weeklyReports.length}件のレポートを生成`);
      
      Logger.log(`週間ランキング処理完了。${weeklyReports.length}件のレポートを生成`);
      
      const result = {
        weeklyData: weeklyAggregated,
        rankings: weeklyRankings,
        reports: weeklyReports
      };
      
      Logger.log(`キー付きで結果を返します: ${Object.keys(result)}`);
      return result;
      
    } catch (error) {
      Logger.log(`週間ランキング処理に失敗: ${error.message}`);
      Logger.log(`エラースタック: ${error.stack}`);
      throw error;
    }
  }
  
  /**
   * 日次データを週間データに集計
   * @param {Array} dailyData - 日次データ
   * @return {Array} 週間集計データ
   */
  aggregateWeeklyData(dailyData) {
    Logger.log("日次データを週間データに集計中...");
    
    // 前週の開始日と終了日を計算
    const previousWeek = this.getPreviousWeekRange();
    Logger.log(`前週のデータをフィルタリング: ${previousWeek.start} から ${previousWeek.end}`);
    
    // 前週のみを含むようにデータをフィルタリング
    const filteredData = dailyData.filter(row => {
      const rowDate = new Date(row.date);
      return rowDate >= previousWeek.start && rowDate <= previousWeek.end;
    });
    
    Logger.log(`前週用に${dailyData.length}行から${filteredData.length}行をフィルタリング`);
    
    const weeklyMap = new Map();
    
    filteredData.forEach(row => {
      const weekKey = this.getWeekKey(row.date);
      
      // 無効な日付の行をスキップ
      if (!weekKey) {
        Logger.log(`無効な日付の行をスキップ: ${row.date}`);
        return;
      }
      
      const rowKey = `${row.searchQuery}|${row.pageUrl}|${row.country}|${row.device}`;
      
      if (!weeklyMap.has(weekKey)) {
        weeklyMap.set(weekKey, new Map());
      }
      
      const weekData = weeklyMap.get(weekKey);
      
      if (!weekData.has(rowKey)) {
        weekData.set(rowKey, {
          week: weekKey,
          searchQuery: row.searchQuery,
          pageUrl: row.pageUrl,
          country: row.country,
          device: row.device,
          clicks: 0,
          impressions: 0,
          ctr: 0,
          position: 0,
          count: 0
        });
      }
      
      const weeklyRow = weekData.get(rowKey);
      weeklyRow.clicks += row.clicks;
      weeklyRow.impressions += row.impressions;
      weeklyRow.position += row.averagePosition;
      weeklyRow.count += 1;
    });
    
    // 週間データを配列に変換して平均を計算
    const weeklyData = [];
    weeklyMap.forEach((weekData, weekKey) => {
      weekData.forEach(row => {
        row.ctr = row.impressions > 0 ? (row.clicks / row.impressions) * 100 : 0;
        row.position = row.count > 0 ? row.position / row.count : 0;
        weeklyData.push(row);
      });
    });
    
    Logger.log(`${weeklyData.length}件の週間データレコードを生成`);
    return weeklyData;
  }
  
  /**
   * 週間ランキングを計算
   * @param {Array} weeklyData - 週間集計データ
   * @return {Object} ランキングデータ
   */
  calculateWeeklyRankings(weeklyData) {
    Logger.log("週間ランキングを計算中...");
    
    const rankings = {
      byClicks: [],
      byImpressions: [],
      byCTR: [],
      byPosition: []
    };
    
    // 週ごとにグループ化
    const weeklyGroups = this.groupByWeek(weeklyData);
    
    Object.keys(weeklyGroups).forEach(weekKey => {
      const weekData = weeklyGroups[weekKey];
      
      // 参照値に基づいて統一ランキングを計算
      // 優先順位: ポジション昇順 → クリック降順 → インプレッション降順 → CTR降順
      const unifiedRanking = weekData
        .sort((a, b) => {
          // 第1: 平均ポジション昇順（低い方が良い）
          if (a.position !== b.position) {
            return a.position - b.position;
          }
          // 第2: クリック降順（高い方が良い）
          if (a.clicks !== b.clicks) {
            return b.clicks - a.clicks;
          }
          // 第3: インプレッション降順（高い方が良い）
          if (a.impressions !== b.impressions) {
            return b.impressions - a.impressions;
          }
          // 第4: CTR降順（高い方が良い）
          return b.ctr - a.ctr;
        })
        .map((row, index) => ({
          ...row,
          ranking: index + 1,
          metric: 'unified'
        }));
      
      // 後方互換性のための個別ランキングを作成
      const clicksRanking = weekData
        .sort((a, b) => b.clicks - a.clicks)
        .map((row, index) => ({
          ...row,
          ranking: index + 1,
          metric: 'clicks'
        }));
      
      const impressionsRanking = weekData
        .sort((a, b) => b.impressions - a.impressions)
        .map((row, index) => ({
          ...row,
          ranking: index + 1,
          metric: 'impressions'
        }));
      
      const ctrRanking = weekData
        .sort((a, b) => b.ctr - a.ctr)
        .map((row, index) => ({
          ...row,
          ranking: index + 1,
          metric: 'ctr'
        }));
      
      const positionRanking = weekData
        .sort((a, b) => a.position - b.position)
        .map((row, index) => ({
          ...row,
          ranking: index + 1,
          metric: 'position'
        }));
      
      // 統一ランキングを主要ランキングシステムとして使用
      rankings.byClicks.push(...unifiedRanking);
      rankings.byImpressions.push(...impressionsRanking);
      rankings.byCTR.push(...ctrRanking);
      rankings.byPosition.push(...positionRanking);
    });
    
    Logger.log("週間ランキング計算完了");
    return rankings;
  }
  
  /**
   * 参照値計算を示す詳細ランキングレポートを作成
   * @param {Array} weeklyData - 週間集計データ
   * @return {Array} 詳細ランキングレポート
   */
  createDetailedRankingReport(weeklyData) {
    Logger.log("参照値付きの詳細ランキングレポートを作成中...");
    
    const report = [];
    
    // 週ごとにグループ化
    const weeklyGroups = this.groupByWeek(weeklyData);
    
    Object.keys(weeklyGroups).forEach(weekKey => {
      const weekData = weeklyGroups[weekKey];
      
      // 参照値でソート
      const sortedData = weekData.sort((a, b) => {
        // 第1: 平均ポジション昇順（低い方が良い）
        if (a.position !== b.position) {
          return a.position - b.position;
        }
        // 第2: クリック降順（高い方が良い）
        if (a.clicks !== b.clicks) {
          return b.clicks - a.clicks;
        }
        // 第3: インプレッション降順（高い方が良い）
        if (a.impressions !== b.impressions) {
          return b.impressions - a.impressions;
        }
        // 第4: CTR降順（高い方が良い）
        return b.ctr - a.ctr;
      });
      
      // ランキング情報を追加
      sortedData.forEach((row, index) => {
        const ranking = index + 1;
        
        report.push({
          week: row.week,
          ranking: ranking,
          searchQuery: row.searchQuery,
          pageUrl: row.pageUrl,
          country: row.country,
          device: row.device,
          clicks: row.clicks,
          impressions: row.impressions,
          ctr: row.ctr,
          position: row.position,
          referenceScore: this.calculateReferenceScore(row),
          rankingReason: this.getRankingReason(row, sortedData, index)
        });
      });
    });
    
    Logger.log(`${report.length}エントリの詳細ランキングレポートを作成`);
    return report;
  }
  
  /**
   * ランキング用の参照スコアを計算
   * @param {Object} row - データ行
   * @return {number} 参照スコア
   */
  calculateReferenceScore(row) {
    try {
      // 参照値に基づく重み付きスコア
      // ポジション: 40%重み（低い方が良いので反転）
      // クリック: 30%重み（高い方が良い）
      // インプレッション: 20%重み（高い方が良い）
      // CTR: 10%重み（高い方が良い）
      
      const positionScore = Math.max(0, 100 - (row.position * 5)); // 最大100、ポジションごとに5減少
      const clicksScore = Math.min(100, (row.clicks / 10) * 100); // 最大100、クリック数に比例
      const impressionsScore = Math.min(100, (row.impressions / 50) * 100); // 最大100、インプレッション数に比例
      const ctrScore = Math.min(100, row.ctr * 1000); // 最大100、CTRに比例
      
      const referenceScore = 
        (positionScore * 0.4) +
        (clicksScore * 0.3) +
        (impressionsScore * 0.2) +
        (ctrScore * 0.1);
      
      return Math.round(referenceScore);
      
    } catch (error) {
      Logger.log(`参照スコア計算エラー: ${error.message}`);
      return 0;
    }
  }
  
  /**
   * 特定の行のランキング理由を取得
   * @param {Object} row - データ行
   * @param {Array} sortedData - ソートされたデータ配列
   * @param {number} index - 現在のインデックス
   * @return {string} ランキング理由
   */
  getRankingReason(row, sortedData, index) {
    try {
      if (index === 0) {
        return "トップパフォーマー - 最高ポジション";
      }
      
      const previousRow = sortedData[index - 1];
      const reasons = [];
      
      // 前の行と比較してランキングを説明
      if (row.position < previousRow.position) {
        reasons.push("より良いポジション");
      } else if (row.position > previousRow.position) {
        reasons.push("より悪いポジション");
      }
      
      if (row.clicks > previousRow.clicks) {
        reasons.push("より多くのクリック");
      } else if (row.clicks < previousRow.clicks) {
        reasons.push("より少ないクリック");
      }
      
      if (row.impressions > previousRow.impressions) {
        reasons.push("より多くのインプレッション");
      } else if (row.impressions < previousRow.impressions) {
        reasons.push("より少ないインプレッション");
      }
      
      if (row.ctr > previousRow.ctr) {
        reasons.push("より高いCTR");
      } else if (row.ctr < previousRow.ctr) {
        reasons.push("より低いCTR");
      }
      
      return reasons.length > 0 ? reasons.join(", ") : "類似のパフォーマンス";
      
    } catch (error) {
      Logger.log(`ランキング理由取得エラー: ${error.message}`);
      return "Unknown";
    }
  }
  
  /**
   * 週間レポートを生成
   * @param {Object} rankings - ランキングデータ
   * @return {Array} 週間レポート
   */
  generateWeeklyReports(rankings) {
    Logger.log("週間レポートを生成中...");
    
    const reports = [];
    
    // 国別週間ランキングレポート（これらのみ保持）
    const countryReports = this.createCountrySpecificWeeklyReports(rankings);
    reports.push(...countryReports);
    
    Logger.log(`${reports.length}件の国別週間レポートを生成`);
    return reports;
  }
  
  /**
   * 国別週間ランキングレポートを作成
   * @param {Object} rankings - ランキングデータ
   * @return {Array} 国別レポート
   */
  createCountrySpecificWeeklyReports(rankings) {
    Logger.log("国別週間レポートを作成中...");
    
    const reports = [];
    
    // データからすべての一意の国を取得
    const countries = new Set();
    rankings.byClicks.forEach(row => {
      if (row.country) {
        countries.add(row.country);
      }
    });
    
    Logger.log(`${countries.size}カ国を発見: ${Array.from(countries).join(', ')}`);
    
    // 各国のレポートを作成
    countries.forEach(country => {
      const countryData = rankings.byClicks.filter(row => row.country === country);
      
      // 現在の週識別子を取得
      const currentWeek = countryData.length > 0 ? countryData[0].week : null;
      
      // トレンド計算用に前週データを取得を試行
      let dataWithTrends = countryData;
      if (currentWeek && WEEKLY_CONFIG.enableTrends) {
        try {
          const previousWeekData = this.getPreviousWeekDataFromSheet(`${country}週間ランキング`, currentWeek);
          if (previousWeekData.length > 0) {
            dataWithTrends = this.calculateTrends(countryData, previousWeekData);
            Logger.log(`前週に基づいて${country}データにトレンドを追加`);
          }
        } catch (error) {
          Logger.log(`${country}のトレンド計算に失敗: ${error.message}`);
        }
      }
      
      // 参照値でソート: Position ASC, then Clicks DESC, then Impressions DESC, then CTR DESC
      const sortedData = dataWithTrends
        .filter(row => row.ranking <= WEEKLY_CONFIG.topRankings)
        .sort((a, b) => {
          // 第1: 平均ポジション昇順（低い方が良い）
          if (a.position !== b.position) {
            return a.position - b.position;
          }
          // 第2: クリック降順（高い方が良い）
          if (a.clicks !== b.clicks) {
            return b.clicks - a.clicks;
          }
          // 第3: インプレッション降順（高い方が良い）
          if (a.impressions !== b.impressions) {
            return b.impressions - a.impressions;
          }
          // 第4: CTR降順（高い方が良い）
          return b.ctr - a.ctr;
        });
      
      const reportData = sortedData.map((row, index) => {
        return [
          row.week,
          row.country,
          row.device,
          row.searchQuery,
          row.pageUrl,
          row.clicks,
          row.impressions,
          row.ctr,
          row.position,
          row.ranking,
          row.trend || '',
          row.clicksChange || '',
          row.impressionsChange || '',
          row.ctrChange || '',
          row.positionChange || ''
        ];
      });
      
      const report = {
        sheetName: `${country}週間ランキング`,
        data: reportData
      };
      
      reports.push(report);
      Logger.log(`${country}週間ランキングレポートを${reportData.length}行で作成`);
    });
    
    Logger.log(`${reports.length}件の国別週間レポートを作成`);
    return reports;
  }
  
  
  /**
   * 週キーを取得（日付間隔形式: YYYY/MM/DD - YYYY/MM/DD）
   * @param {Date} date - 日付
   * @return {string} "YYYY/MM/DD - YYYY/MM/DD"形式の週キー
   */
  getWeekKey(date) {
    try {
      const d = new Date(date);
      
      // 日付を検証
      if (isNaN(d.getTime())) {
        Logger.log(`無効な日付を受信: ${date}`);
        return null;
      }
      
      // 週の開始日と終了日を計算
      const dayOfWeek = d.getDay();
      const weekStartDay = WEEKLY_CONFIG.weekStartDay || 1; // Default to Monday
      
      // 週の開始日を取得するために減算する日数を計算
      let daysToSubtract = dayOfWeek - weekStartDay;
      if (daysToSubtract < 0) {
        daysToSubtract += 7; // 月曜日から始まる週に調整
      }
      
      const weekStart = new Date(d);
      weekStart.setDate(d.getDate() - daysToSubtract);
      weekStart.setHours(0, 0, 0, 0);
      
      const weekEnd = new Date(weekStart);
      weekEnd.setDate(weekStart.getDate() + 6);
      weekEnd.setHours(23, 59, 59, 999);
      
      // "YYYY/MM/DD - YYYY/MM/DD"形式でフォーマット
      const formatDate = (dateObj) => {
        const year = dateObj.getFullYear();
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const day = String(dateObj.getDate()).padStart(2, '0');
        return `${year}/${month}/${day}`;
      };
      
      const weekKey = `${formatDate(weekStart)} - ${formatDate(weekEnd)}`;
      
      // Logger.log(`日付: ${date} -> 週キー: ${weekKey}`); // Commented out to reduce log noise
      return weekKey;
      
    } catch (error) {
      Logger.log(`日付${date}の週キー取得エラー: ${error.message}`);
      return null;
    }
  }
  
  /**
   * 前週の開始日と終了日を取得
   * @return {Object} 前週の開始日と終了日を含むオブジェクト
   */
  getPreviousWeekRange() {
    const today = new Date();
    const weekStartDay = WEEKLY_CONFIG.weekStartDay || 1; // Default to Monday
    
    // 現在の週の開始を計算
    const currentDayOfWeek = today.getDay();
    let daysToSubtract = currentDayOfWeek - weekStartDay;
    if (daysToSubtract < 0) {
      daysToSubtract += 7;
    }
    
    const currentWeekStart = new Date(today);
    currentWeekStart.setDate(today.getDate() - daysToSubtract);
    currentWeekStart.setHours(0, 0, 0, 0);
    
    // 前週を計算（現在の週開始の7日前）
    const previousWeekStart = new Date(currentWeekStart);
    previousWeekStart.setDate(currentWeekStart.getDate() - 7);
    
    const previousWeekEnd = new Date(previousWeekStart);
    previousWeekEnd.setDate(previousWeekStart.getDate() + 6);
    previousWeekEnd.setHours(23, 59, 59, 999);
    
    return {
      start: previousWeekStart,
      end: previousWeekEnd
    };
  }

  /**
   * Get specific week data for October 13-19, 2025
   * @return {Object} Object with start and end dates for Oct 13-19 week
   */
  getOct13To19WeekRange() {
        // 特定の週を指定: 2025年10月13日-19日
        const specificWeekStart = new Date(2025, 9, 13); // October 13, 2025 (month is 0-based)
        specificWeekStart.setHours(0, 0, 0, 0);

        const specificWeekEnd = new Date(2025, 9, 19); // October 19, 2025
        specificWeekEnd.setHours(23, 59, 59, 999);
    
    Logger.log(`Getting data for specific week: ${specificWeekStart.toDateString()} to ${specificWeekEnd.toDateString()}`);
    
    return {
      start: specificWeekStart,
      end: specificWeekEnd
    };
  }
  
  /**
   * トレンド計算用に既存シートから前週データを取得
   * @param {string} sheetName - シート名
   * @param {string} currentWeek - 現在の週識別子
   * @return {Array} 前週のデータ
   */
  getPreviousWeekDataFromSheet(sheetName, currentWeek) {
    try {
      const spreadsheet = getOrCreateSpreadsheet();
      const sheet = spreadsheet.getSheetByName(sheetName);
      
      if (!sheet) {
        Logger.log(`前週データ用のシート${sheetName}が見つかりません`);
        return [];
      }
      
      const lastRow = sheet.getLastRow();
      if (lastRow <= 1) {
        Logger.log(`シート${sheetName}にデータがありません`);
        return [];
      }
      
      // 前週を見つけるために現在の週を解析
      let currentWeekDate = null;
      if (currentWeek.toString().includes(' - ')) {
        const startDateStr = currentWeek.toString().split(' - ')[0].trim();
        currentWeekDate = new Date(startDateStr);
      } else {
        currentWeekDate = new Date(currentWeek);
      }
      
      // 前週の範囲を計算（7日前）
      const previousWeekStart = new Date(currentWeekDate);
      previousWeekStart.setDate(previousWeekStart.getDate() - 7);
      previousWeekStart.setHours(0, 0, 0, 0);
      
      const previousWeekEnd = new Date(previousWeekStart);
      previousWeekEnd.setDate(previousWeekStart.getDate() + 6);
      previousWeekEnd.setHours(23, 59, 59, 999);
      
      // 前週キーをフォーマット
      const formatDate = (dateObj) => {
        const year = dateObj.getFullYear();
        const month = String(dateObj.getMonth() + 1).padStart(2, '0');
        const day = String(dateObj.getDate()).padStart(2, '0');
        return `${year}/${month}/${day}`;
      };
      
      const previousWeekKey = `${formatDate(previousWeekStart)} - ${formatDate(previousWeekEnd)}`;
      
      Logger.log(`前週を検索中: ${previousWeekKey}`);
      
      // すべてのデータを読み取り、前週でフィルタリング
      const dataRange = sheet.getRange(2, 1, lastRow - 1, 10);
      const rawData = dataRange.getValues();
      
      const previousWeekData = rawData
        .filter(row => row[0] === previousWeekKey)
        .map(row => ({
          week: row[0],
          country: row[1],
          device: row[2],
          searchQuery: row[3],
          pageUrl: row[4],
          clicks: parseInt(row[5]) || 0,
          impressions: parseInt(row[6]) || 0,
          ctr: parseFloat(row[7]) || 0,
          position: parseFloat(row[8]) || 0,
          ranking: parseInt(row[9]) || 0
        }));
      
      Logger.log(`前週${previousWeekKey}のレコードを${previousWeekData.length}件発見`);
      return previousWeekData;
      
    } catch (error) {
      Logger.log(`前週データ取得エラー: ${error.message}`);
      return [];
    }
  }

  /**
   * 履歴データのトレンド（週間変化）を計算
   * @param {Array} currentWeekData - 今週のデータ
   * @param {Array} previousWeekData - 前週のデータ
   * @return {Array} トレンド情報付きデータ
   */
  calculateTrends(currentWeekData, previousWeekData) {
    Logger.log("週間トレンドを計算中...");
    
    // 前週データのクイックルックアップ用マップを作成
    const previousWeekMap = new Map();
    previousWeekData.forEach(row => {
      const key = `${row.searchQuery}|${row.pageUrl}|${row.country}|${row.device}`;
      previousWeekMap.set(key, row);
    });
    
    // 今週の各レコードのトレンドを計算
    const trends = currentWeekData.map(currentRow => {
      const key = `${currentRow.searchQuery}|${currentRow.pageUrl}|${currentRow.country}|${currentRow.device}`;
      const previousRow = previousWeekMap.get(key);
      
      if (!previousRow) {
        // 今週の新規エントリ
        return {
          ...currentRow,
          trend: 'new',
          clicksChange: currentRow.clicks,
          impressionsChange: currentRow.impressions,
          ctrChange: currentRow.ctr,
          positionChange: -(currentRow.averagePosition - 100), // より悪いポジションは100と仮定
          rankingChange: null // 前のランキングなし
        };
      }
      
      // 変化を計算
      const clicksChange = currentRow.clicks - previousRow.clicks;
      const impressionsChange = currentRow.impressions - previousRow.impressions;
      const ctrChange = currentRow.ctr - previousRow.ctr;
      const positionChange = previousRow.position - currentRow.position; // 正の値は良い
      const rankingChange = previousRow.ranking - currentRow.ranking; // 低い数値は良い
      
      // トレンド方向を決定
      let trend = 'stable';
      if (clicksChange > 0 && impressionsChange > 0 && positionChange > 0) {
        trend = 'up';
      } else if (clicksChange < 0 || impressionsChange < 0 || positionChange < 0 || rankingChange < 0) {
        trend = 'down';
      }
      
      return {
        ...currentRow,
        trend: trend,
        clicksChange: clicksChange,
        impressionsChange: impressionsChange,
        ctrChange: ctrChange,
        positionChange: positionChange,
        rankingChange: rankingChange,
        previousWeek: {
          clicks: previousRow.clicks,
          impressions: previousRow.impressions,
          ctr: previousRow.ctr,
          position: previousRow.position,
          ranking: previousRow.ranking
        }
      };
    });
    
    Logger.log(`${trends.length}レコードのトレンドを計算`);
    return trends;
  }
  
  /**
   * 国別でデータをフィルタリング
   * @param {Array} data - フィルタリングするデータ
   * @param {string} country - フィルタリングする国
   * @return {Array} フィルタリングされたデータ
   */
  filterByCountry(data, country) {
    if (!country) {
      return data;
    }
    return data.filter(row => row.country === country);
  }
  
  /**
   * 検索クエリ別でデータをフィルタリング
   * @param {Array} data - フィルタリングするデータ
   * @param {string} query - フィルタリングする検索クエリ
   * @return {Array} フィルタリングされたデータ
   */
  filterByQuery(data, query) {
    if (!query) {
      return data;
    }
    const lowerQuery = query.toLowerCase();
    return data.filter(row => row.searchQuery.toLowerCase().includes(lowerQuery));
  }
  
  /**
   * 週別でデータをグループ化
   * @param {Array} data - データ
   * @return {Object} 週別グループ
   */
  groupByWeek(data) {
    const groups = {};
    data.forEach(row => {
      if (!groups[row.week]) {
        groups[row.week] = [];
      }
      groups[row.week].push(row);
    });
    return groups;
  }

  /**
   * Process data specifically for October 13-19, 2024 week
   * @param {Array} dailyData - Daily data
   * @return {Object} Weekly ranking data for Oct 13-19 week
   */
  processOct13To19WeekData(dailyData) {
    Logger.log("10月13-19日の週間データを処理中...");
    
    try {
      // 特定の週（10月13-19日）のデータをフィルタリング
      const octWeek = this.getOct13To19WeekRange();
      Logger.log(`10月13-19日のデータをフィルタリング: ${octWeek.start} から ${octWeek.end}`);
      
      // 10月13-19日のデータのみをフィルタリング
      const filteredData = dailyData.filter(row => {
        const rowDate = new Date(row.date);
        return rowDate >= octWeek.start && rowDate <= octWeek.end;
      });
      
      Logger.log(`10月13-19日用に${dailyData.length}行から${filteredData.length}行をフィルタリング`);
      
      const weeklyMap = new Map();
      
      filteredData.forEach(row => {
        const weekKey = "2025/10/13 - 2025/10/19"; // 固定の週キー
        
        // 無効な日付の行をスキップ
        if (!weekKey) {
          Logger.log(`無効な日付の行をスキップ: ${row.date}`);
          return;
        }
        
        const rowKey = `${row.searchQuery}|${row.pageUrl}|${row.country}|${row.device}`;
        
        if (!weeklyMap.has(weekKey)) {
          weeklyMap.set(weekKey, new Map());
        }
        
        const weekData = weeklyMap.get(weekKey);
        
        if (!weekData.has(rowKey)) {
          weekData.set(rowKey, {
            week: weekKey,
            searchQuery: row.searchQuery,
            pageUrl: row.pageUrl,
            country: row.country,
            device: row.device,
            clicks: 0,
            impressions: 0,
            ctr: 0,
            position: 0,
            count: 0
          });
        }
        
        const weeklyRow = weekData.get(rowKey);
        weeklyRow.clicks += row.clicks;
        weeklyRow.impressions += row.impressions;
        weeklyRow.position += row.averagePosition;
        weeklyRow.count += 1;
      });
      
      // 週間データを配列に変換して平均を計算
      const weeklyData = [];
      weeklyMap.forEach((weekData, weekKey) => {
        weekData.forEach(row => {
          row.ctr = row.impressions > 0 ? (row.clicks / row.impressions) * 100 : 0;
          row.position = row.count > 0 ? row.position / row.count : 0;
          weeklyData.push(row);
        });
      });
      
      Logger.log(`${weeklyData.length}件の10月13-19日週間データレコードを生成`);
      
      // 週間ランキングを計算
      const weeklyRankings = this.calculateWeeklyRankings(weeklyData);
      
      // 週間レポートを生成
      const weeklyReports = this.generateWeeklyReports(weeklyRankings);
      
      const result = {
        weeklyData: weeklyData,
        rankings: weeklyRankings,
        reports: weeklyReports,
        weekRange: octWeek
      };
      
      Logger.log(`10月13-19日週間データ処理完了。${weeklyReports.length}件のレポートを生成`);
      return result;
      
    } catch (error) {
      Logger.log(`10月13-19日週間データ処理に失敗: ${error.message}`);
      throw error;
    }
  }
}

/**
 * Process October 13-19, 2024 week data specifically
 * Can be executed independently to get data for that specific week
 */
function runOct13To19WeekProcessor() {
  try {
    Logger.log("=== 10月13-19日週間データ処理を開始 ===");
    
    // Get daily data from existing spreadsheet
    Logger.log("日次データを取得中...");
    const dailyData = getDailyDataFromSpreadsheet();
    
    if (!dailyData || dailyData.length === 0) {
      Logger.log("日次データが見つかりません。まずメイン関数を実行してください。");
      return;
    }
    
    Logger.log(`${dailyData.length}件の日次データレコードを取得`);
    
    // Process October 13-19 week data
    Logger.log("10月13-19日週間データを処理中...");
    const weeklyProcessor = new WeeklyProcessor();
    let octWeekResults;
    
    try {
      octWeekResults = weeklyProcessor.processOct13To19WeekData(dailyData);
    } catch (processingError) {
      Logger.log(`10月13-19日週間処理に失敗: ${processingError.message}`);
      Logger.log("フォールバック空の結果を作成中...");
      octWeekResults = {
        weeklyData: [],
        rankings: {},
        trends: {},
        reports: [],
        weekRange: { start: new Date(2025, 9, 13), end: new Date(2025, 9, 19) }
      };
    }
    
    // Debug: Log the structure of octWeekResults
    Logger.log("=== 10月13-19日週間データ処理結果 ===");
    Logger.log(`octWeekResults type: ${typeof octWeekResults}`);
    Logger.log(`octWeekResults keys: ${Object.keys(octWeekResults || {})}`);
    
    if (octWeekResults) {
      Logger.log(`Weekly data count: ${octWeekResults.weeklyData ? octWeekResults.weeklyData.length : 'undefined'}`);
      Logger.log(`Ranking count: ${octWeekResults.rankings ? Object.keys(octWeekResults.rankings).length : 'undefined'}`);
      Logger.log(`Report count: ${octWeekResults.reports ? octWeekResults.reports.length : 'undefined'}`);
    } else {
      Logger.log("エラー: octWeekResultsがnullまたは未定義です");
      return;
    }
    
    // Export October 13-19 week data to spreadsheet
    Logger.log("10月13-19日週間データをスプレッドシートにエクスポート中...");
    exportWeeklyRankingsToSpreadsheet(octWeekResults);
    
    // Update URL average ranking sheet for this specific week
    updateUrlAverageRankingSheet(octWeekResults, dailyData);
    
    Logger.log("=== 10月13-19日週間データ処理完了 ===");
    
  } catch (error) {
    Logger.log(`10月13-19日週間データ処理エラー: ${error.message}`);
    throw error;
  }
}

/**
 * 週間ランキング処理を独立して実行
 * メイン関数とは別に実行可能
 */
function runWeeklyProcessor() {
  try {
    Logger.log("=== 完全な週間ランキング処理を開始 ===");
    Logger.log(`WEEKLY_CONFIG: ${WEEKLY_CONFIG ? 'defined' : 'undefined'}`);
    if (WEEKLY_CONFIG) {
      Logger.log(`WEEKLY_CONFIG.enabled: ${WEEKLY_CONFIG.enabled}`);
    }

    // Check configuration
    if (!WEEKLY_CONFIG || !WEEKLY_CONFIG.enabled) {
      Logger.log("週間ランキング機能が無効です。設定を確認してください。");
      return;
    }
    
    // ステップ1: トレンド列を含むヘッダーを更新
    Logger.log("\nステップ1: シートヘッダーを更新中...");
    updateWeeklyRankingHeaders();
    
    // ステップ2: 既存スプレッドシートから日次データを取得
    Logger.log("\nステップ2: 日次データを取得中...");
    const dailyData = getDailyDataFromSpreadsheet();
    
    if (!dailyData || dailyData.length === 0) {
      Logger.log("日次データが見つかりません。まずメイン関数を実行してください。");
      return;
    }
    
    Logger.log(`Retrieved ${dailyData.length} daily data records`);
    
    // ステップ3: 週間ランキング処理を実行
    Logger.log("\nステップ3: 週間ランキングを処理中...");
    const weeklyProcessor = new WeeklyProcessor();
    let weeklyResults;
    
    try {
      weeklyResults = weeklyProcessor.processWeeklyRankings(dailyData);
    } catch (processingError) {
      Logger.log(`週間処理に失敗: ${processingError.message}`);
      Logger.log("フォールバック空の結果を作成中...");
      weeklyResults = {
        weeklyData: [],
        rankings: {},
        trends: {},
        reports: []
      };
    }
    
    // デバッグ: weeklyResultsの構造をログ出力
    Logger.log("=== 週間ランキング処理結果 ===");
    Logger.log(`weeklyResults type: ${typeof weeklyResults}`);
    Logger.log(`weeklyResults keys: ${Object.keys(weeklyResults || {})}`);
    
    if (weeklyResults) {
      Logger.log(`Weekly data count: ${weeklyResults.weeklyData ? weeklyResults.weeklyData.length : 'undefined'}`);
      Logger.log(`Ranking count: ${weeklyResults.rankings ? Object.keys(weeklyResults.rankings).length : 'undefined'}`);
      Logger.log(`Report count: ${weeklyResults.reports ? weeklyResults.reports.length : 'undefined'}`);
    } else {
      Logger.log("エラー: weeklyResultsがnullまたは未定義です");
      return;
    }
    
    // ステップ4: 週間ランキングデータをスプレッドシートにエクスポート
    Logger.log("\nステップ4: 週間ランキングデータをエクスポート中...");
    exportWeeklyRankingsToSpreadsheet(weeklyResults);
    
    // ステップ4.5: URL平均掲載順位シートを更新
    Logger.log("\nステップ4.5: URL平均掲載順位シートを更新中...");
    updateUrlAverageRankingSheet(weeklyResults, dailyData);
    
    // ステップ4.6: 国別URL平均掲載順位シートを更新
    Logger.log("\nステップ4.6: 国別URL平均掲載順位シートを更新中...");
    createCountryUrlAverageSheets(weeklyResults, dailyData);
    
    // ステップ5: 有効な場合にチャートを生成
    if (WEEKLY_CONFIG.historicalTracking) {
      Logger.log("\nステップ5: トレンドチャートを生成中...");
      try {
        generateWeeklyCharts();
      } catch (chartError) {
        Logger.log(`チャート生成に失敗: ${chartError.message}`);
        // チャートが失敗してもプロセス全体を失敗させない
      }
    }
    
    Logger.log("=== 完全な週間ランキング処理完了 ===");
    
  } catch (error) {
    Logger.log(`週間ランキング処理エラー: ${error.message}`);
    throw error;
  }
}

/**
 * Update weekly ranking sheet headers to include trend columns
 */
function updateWeeklyRankingHeaders() {
  try {
    Logger.log("Updating weekly ranking sheet headers...");
    
    const spreadsheet = getOrCreateSpreadsheet();
    
    // Find all weekly ranking sheet templates
    const countryTemplates = SPREADSHEET_TEMPLATE.sheets.filter(s => 
      s.name.includes("週間ランキング")
    );
    
    Logger.log(`Found ${countryTemplates.length} weekly ranking sheet templates`);
    
    countryTemplates.forEach(template => {
      const sheetName = template.name;
      const sheet = spreadsheet.getSheetByName(sheetName);
      
      if (sheet) {
        Logger.log(`Updating ${sheetName}...`);
        
        Logger.log(`  Old headers: ${sheet.getLastColumn()} columns`);
        
        // Update headers
        sheet.getRange(1, 1, 1, template.columns.length).setValues([template.columns]);
        sheet.getRange(1, 1, 1, template.columns.length).setBackground('#d9ead3'); // Light green background
        
        Logger.log(`  New headers: ${template.columns.length} columns`);
        Logger.log(`  Columns: ${template.columns.join(', ')}`);
      } else {
        Logger.log(`Sheet ${sheetName} not found - skipping`);
      }
    });
    
    Logger.log("Header update completed!");
    
  } catch (error) {
    Logger.log(`Error updating headers: ${error.message}`);
    throw error;
  }
}

/**
 * Generate charts for all weekly ranking sheets
 */
function generateWeeklyCharts() {
  try {
    Logger.log("=== Generating Weekly Trend Charts ===");
    
    const spreadsheet = getOrCreateSpreadsheet();
    const countries = ["米国", "カナダ", "イギリス", "オーストラリア", "ニュージーランド", "シンガポール"];
    
    countries.forEach(country => {
      const sheetName = `${country}週間ランキング`;
      let sheet = spreadsheet.getSheetByName(sheetName);
      
      if (!sheet) {
        Logger.log(`${sheetName} not found, creating sheet...`);
        sheet = spreadsheet.insertSheet(sheetName);
        
        // Add headers using template
        const template = SPREADSHEET_TEMPLATE.sheets.find(s => s.name.includes("週間ランキング"));
        if (template) {
          sheet.getRange(1, 1, 1, template.columns.length).setValues([template.columns]);
          sheet.getRange(1, 1, 1, template.columns.length).setBackground('#d9ead3'); // Light green background
          Logger.log(`Created ${sheetName} with headers`);
        }
        
        // Try to generate weekly data for this country
        try {
          Logger.log(`Generating weekly data for ${country}...`);
          const dailyData = getDailyDataFromSpreadsheet();
          if (dailyData && dailyData.length > 0) {
            const weeklyProcessor = new WeeklyProcessor();
            const weeklyResults = weeklyProcessor.processWeeklyRankings(dailyData);
            
            // Find the report for this country
            const countryReport = weeklyResults.reports.find(report => report.sheetName === sheetName);
            if (countryReport && countryReport.data && countryReport.data.length > 0) {
              // Add the data to the sheet
              sheet.getRange(2, 1, countryReport.data.length, countryReport.data[0].length).setValues(countryReport.data);
              Logger.log(`Added ${countryReport.data.length} rows of data to ${sheetName}`);
            }
          }
        } catch (dataError) {
          Logger.log(`Could not generate data for ${country}: ${dataError.message}`);
        }
      }
      
      const lastRow = sheet.getLastRow();
      if (lastRow <= 1) {
        Logger.log(`${sheetName} has no data, skipping chart creation`);
        return;
      }
      
      // Get all historical data
      const data = sheet.getRange(2, 1, lastRow - 1, 16).getValues();
      
      // Create charts
      Logger.log(`Creating charts for ${sheetName} with ${data.length} records...`);
      createTrendCharts(data, sheetName);
    });
    
    Logger.log("=== All Charts Generated Successfully ===");
    
  } catch (error) {
    Logger.log(`Error generating charts: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
    throw error;
  }
}

/**
 * Get or create the main spreadsheet
 * @return {Object} Google Spreadsheet object
 */
function getOrCreateSpreadsheet() {
  try {
    // Use the user's specific spreadsheet ID
    const SPREADSHEET_ID = "1oIyrC36E2WCLA9Sys4X3EB8SKKIPnVccxRbgkKpuv7o";
    
    Logger.log(`Using user's specific spreadsheet ID: ${SPREADSHEET_ID}`);
    const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
    
    Logger.log(`Successfully opened spreadsheet: "${spreadsheet.getName()}"`);
    Logger.log(`Spreadsheet URL: ${spreadsheet.getUrl()}`);
    
    return spreadsheet;
    
  } catch (error) {
    Logger.log(`Error opening user's spreadsheet: ${error.message}`);
    Logger.log(`Please check that the spreadsheet ID is correct and the service account has access to it`);
    throw error;
  }
}

/**
 * Get daily data from spreadsheet
 * @return {Array} Daily data
 */
function getDailyDataFromSpreadsheet() {
  try {
    Logger.log("Getting daily data from spreadsheet...");
    
    // Get existing spreadsheet
    const spreadsheet = getOrCreateSpreadsheet();
    Logger.log(`Spreadsheet found: ${spreadsheet.getName()}`);
    
    const allSitesSheet = spreadsheet.getSheetByName("全サイトデータ");
    
    if (!allSitesSheet) {
      Logger.log("全サイトデータ sheet not found");
      Logger.log("Available sheets:");
      const sheets = spreadsheet.getSheets();
      sheets.forEach(sheet => {
        Logger.log(`- ${sheet.getName()}`);
      });
      return [];
    }
    
    // Get data (excluding header row)
    const lastRow = allSitesSheet.getLastRow();
    const lastCol = allSitesSheet.getLastColumn();
    Logger.log(`全サイトデータ sheet has ${lastRow} rows, ${lastCol} columns`);
    
    if (lastRow <= 1) {
      Logger.log("No data available in 全サイトデータ sheet");
      return [];
    }
    
    // Check what's actually in the sheet
    Logger.log("Checking actual data in 全サイトデータ sheet...");
    const sampleRange = allSitesSheet.getRange(1, 1, Math.min(5, lastRow), lastCol);
    const sampleData = sampleRange.getValues();
    Logger.log(`Sample data (first ${Math.min(5, lastRow)} rows): ${JSON.stringify(sampleData)}`);
    
    const dataRange = allSitesSheet.getRange(2, 1, lastRow - 1, 10);
    const rawData = dataRange.getValues();
    
    Logger.log(`Raw data retrieved: ${rawData.length} rows`);
    if (rawData.length > 0) {
      Logger.log(`First raw data row: ${JSON.stringify(rawData[0])}`);
    }
    
    // Transform data
    const dailyData = rawData.map(row => ({
      site: row[0],
      date: parseJapaneseDate(row[1]),
      searchQuery: row[2],
      pageUrl: row[3],
      country: row[4],
      device: row[5],
      clicks: parseInt(row[6]) || 0,
      impressions: parseInt(row[7]) || 0,
      ctr: parseFloat(row[8]) || 0,
      averagePosition: parseFloat(row[9]) || 0
    }));
    
    Logger.log(`Retrieved ${dailyData.length} daily data records`);
    return dailyData;
    
  } catch (error) {
    Logger.log(`Daily data retrieval error: ${error.message}`);
    return [];
  }
}

/**
 * Get existing weeks from a sheet (to check for duplicates)
 * @param {Object} sheet - Google Sheet object
 * @return {Object} Object with uniqueWeeks array and latestWeek date
 */
function getExistingWeeks(sheet) {
  try {
    const lastRow = sheet.getLastRow();
    if (lastRow <= 1) {
      return {
        uniqueWeeks: [],
        latestWeek: null,
        weekCounts: {}
      }; // No data rows
    }
    
    // Week is in the first column (column 1)
    const weekColumn = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
    const uniqueWeeks = new Set();
    const weekCounts = {};
    let latestDate = null;
    
    weekColumn.forEach(row => {
      if (row[0] && row[0] !== '') {
        const weekStr = row[0].toString();
        uniqueWeeks.add(weekStr);
        
        // Count occurrences
        weekCounts[weekStr] = (weekCounts[weekStr] || 0) + 1;
        
        // Try to parse as date to find latest
        // Handle date interval format: "YYYY/MM/DD - YYYY/MM/DD"
        try {
          let weekDate = null;
          
          // Check if it's a date interval format
          if (weekStr.includes(' - ')) {
            // Extract the start date from the interval
            const startDateStr = weekStr.split(' - ')[0].trim();
            weekDate = new Date(startDateStr);
          } else {
            // Try parsing as regular date
            weekDate = new Date(weekStr);
          }
          
          if (!isNaN(weekDate.getTime())) {
            if (!latestDate || weekDate > latestDate) {
              latestDate = weekDate;
            }
          }
        } catch (e) {
          // Not a date, skip
        }
      }
    });
    
    return {
      uniqueWeeks: Array.from(uniqueWeeks),
      latestWeek: latestDate,
      weekCounts: weekCounts
    };
    
  } catch (error) {
    Logger.log(`Error getting existing weeks: ${error.message}`);
    return {
      uniqueWeeks: [],
      latestWeek: null,
      weekCounts: {}
    };
  }
}

/**
 * Export weekly ranking data to spreadsheet
 * @param {Object} weeklyResults - Weekly ranking results
 */
function exportWeeklyRankingsToSpreadsheet(weeklyResults) {
  try {
    Logger.log("Exporting weekly ranking data to spreadsheet...");
    
    // Validate weeklyResults
    if (!weeklyResults) {
      Logger.log("Error: weeklyResults is undefined");
      return;
    }
    
    if (!weeklyResults.reports) {
      Logger.log("Error: weeklyResults.reports is undefined");
      Logger.log(`weeklyResults structure: ${JSON.stringify(Object.keys(weeklyResults))}`);
      return;
    }
    
    Logger.log(`Processing ${weeklyResults.reports.length} weekly reports`);
    
    // Get existing spreadsheet
    const spreadsheet = getOrCreateSpreadsheet();
    
    // Process weekly ranking sheets
    weeklyResults.reports.forEach((report, index) => {
      Logger.log(`Processing report ${index + 1}: ${report.sheetName}`);
      Logger.log(`Report data length: ${report.data ? report.data.length : 'undefined'}`);
      
      const sheetName = report.sheetName;
      let sheet = spreadsheet.getSheetByName(sheetName);
      
      if (!sheet) {
        Logger.log(`Creating ${sheetName} sheet...`);
        sheet = spreadsheet.insertSheet(sheetName);
      }
      
      // Find template for this sheet
      let template = SPREADSHEET_TEMPLATE.sheets.find(s => s.name === sheetName);
      
      // If not found and it's a country-specific sheet, use the first country template as fallback
      if (!template && sheetName.includes("週間ランキング")) {
        template = SPREADSHEET_TEMPLATE.sheets.find(s => s.name.includes("週間ランキング"));
        if (template) {
          Logger.log(`Using ${template.name} template for ${sheetName}`);
        }
      }
      
      // Check if headers exist and if they match the template
      const lastRow = sheet.getLastRow();
      const hasHeaders = lastRow >= 1;
      const needsHeaderUpdate = hasHeaders && template;
      
      if (needsHeaderUpdate) {
        // Check if headers need updating by comparing column count
        const currentColumnCount = sheet.getLastColumn();
        const expectedColumnCount = template.columns.length;
        
        if (currentColumnCount !== expectedColumnCount) {
          Logger.log(`Updating headers for ${sheetName}: ${currentColumnCount} cols -> ${expectedColumnCount} cols`);
          // Update headers to match the template
          sheet.getRange(1, 1, 1, expectedColumnCount).setValues([template.columns]);
          sheet.getRange(1, 1, 1, expectedColumnCount).setBackground('#d9ead3'); // Light green background
          Logger.log(`Headers updated successfully for ${sheetName}`);
        }
      }
      
      // Add headers if sheet is empty
      const needsHeaders = lastRow === 0 || !sheet.getRange(1, 1, 1, 1).getValue();
      
      if (needsHeaders) {
        Logger.log(`Adding headers for ${sheetName}...`);
        
        if (template) {
          Logger.log(`Setting headers for ${sheetName}: ${template.columns.join(', ')}`);
          sheet.getRange(1, 1, 1, template.columns.length).setValues([template.columns]);
          sheet.getRange(1, 1, 1, template.columns.length).setBackground('#d9ead3'); // Light green background
          Logger.log(`Headers set successfully for ${sheetName}`);
        } else {
          Logger.log(`No template found for ${sheetName}`);
        }
      }
      
      // PHASE 2: APPEND-ONLY WEEKLY HISTORY LOGIC
      // Detect the latest existing week, append new data if newer, prevent overwrites
      if (report.data && report.data.length > 0) {
        // Get the week from the first row of data
        const newWeek = report.data[0][0]; // Week is in the first column
        
        Logger.log(`\n=== Processing Week: ${newWeek} for ${sheetName} ===`);
        
        // Get existing weeks and latest week from the sheet
        const weekInfo = getExistingWeeks(sheet);
        const existingWeeks = weekInfo.uniqueWeeks;
        const latestWeek = weekInfo.latestWeek;
        const weekCounts = weekInfo.weekCounts;
        
        Logger.log(`Existing weeks in ${sheetName}: ${existingWeeks.length} unique weeks`);
        if (existingWeeks.length > 0) {
          Logger.log(`Existing weeks: ${existingWeeks.join(', ')}`);
          Logger.log(`Week row counts: ${JSON.stringify(weekCounts)}`);
        }
        if (latestWeek) {
          Logger.log(`Latest existing week: ${latestWeek}`);
        }
        
        // Check if this week already exists in the sheet
        const weekAlreadyExists = existingWeeks.includes(newWeek.toString());
        
        if (weekAlreadyExists) {
          // Week already exists - UPDATE existing data
          Logger.log(`⚠️ Week ${newWeek} already exists in ${sheetName}`);
          Logger.log(`📝 Row count for this week: ${weekCounts[newWeek] || 0} rows`);
          Logger.log(`🔄 Updating existing week data...`);
          
          // Find the rows for this week and replace them
          const weekColumn = sheet.getRange(1, 1, sheet.getLastRow(), 1).getValues();
          let weekStartRow = -1;
          let weekEndRow = -1;
          
          // Find the start and end rows for this week
          for (let i = 0; i < weekColumn.length; i++) {
            if (weekColumn[i][0] && weekColumn[i][0].toString() === newWeek.toString()) {
              if (weekStartRow === -1) {
                weekStartRow = i + 1; // Convert to 1-based row index
              }
              weekEndRow = i + 1; // Update end row as we find more rows
            }
          }
          
          if (weekStartRow !== -1 && weekEndRow !== -1) {
            const existingRowCount = weekEndRow - weekStartRow + 1;
            const newRowCount = report.data.length;
            
            Logger.log(`📊 Found existing week data at rows ${weekStartRow}-${weekEndRow} (${existingRowCount} rows)`);
            Logger.log(`📊 New data has ${newRowCount} rows`);
            
            // Delete existing rows for this week
            if (existingRowCount > 0) {
              sheet.deleteRows(weekStartRow, existingRowCount);
              Logger.log(`🗑️ Deleted ${existingRowCount} existing rows for week ${newWeek}`);
            }
            
            // Insert new data at the same position
            try {
              sheet.insertRowsBefore(weekStartRow, newRowCount);
              sheet.getRange(weekStartRow, 1, newRowCount, report.data[0].length).setValues(report.data);
              Logger.log(`✅ Successfully updated week ${newWeek} with ${newRowCount} rows`);
            } catch (updateError) {
              Logger.log(`❌ Error updating week ${newWeek}: ${updateError.message}`);
            }
          } else {
            Logger.log(`❌ Could not find existing week data rows for ${newWeek}`);
          }
        } else {
          // New week - check if it's newer than the latest existing week
          // Extract start date from interval format: "YYYY/MM/DD - YYYY/MM/DD"
          let newWeekDate = null;
          try {
            if (newWeek.toString().includes(' - ')) {
              const startDateStr = newWeek.toString().split(' - ')[0].trim();
              newWeekDate = new Date(startDateStr);
            } else {
              newWeekDate = new Date(newWeek);
            }
          } catch (e) {
            Logger.log(`Error parsing newWeek date: ${e.message}`);
            newWeekDate = new Date(newWeek);
          }
          
          const isNewerWeek = !latestWeek || isNaN(newWeekDate.getTime()) || newWeekDate >= latestWeek;
          
          if (isNewerWeek || !latestWeek) {
            Logger.log(`✅ Week ${newWeek} is NEW - inserting at TOP of ${sheetName}...`);
            if (latestWeek) {
              Logger.log(`   (Newer than latest existing week: ${latestWeek})`);
            }
            
            // Check if sheet has any data beyond headers
            const lastRow = sheet.getLastRow();
            const hasData = lastRow > 1;
            
            if (hasData) {
              // Sheet has data - we need to INSERT rows and shift existing data down
              Logger.log(`📊 Sheet has ${lastRow - 1} existing data rows - inserting ${report.data.length} new rows at TOP`);
              
              try {
                // Insert blank rows for the new data (starting at row 2, after header)
                sheet.insertRows(2, report.data.length);
                Logger.log(`📝 Inserted ${report.data.length} blank rows at row 2`);
                
                // Write the new data to the newly inserted rows
                sheet.getRange(2, 1, report.data.length, report.data[0].length).setValues(report.data);
                Logger.log(`✅ Successfully inserted ${report.data.length} new rows at TOP of ${sheetName}`);
                
                // Auto-resize columns after data is written
                // Auto-resize disabled
                Logger.log(`📏 Columns auto-resized`);
                
                // Verify data was written
                const writtenData = sheet.getRange(2, 1, Math.min(3, report.data.length), report.data[0].length).getValues();
                Logger.log(`✓ Verification - first 3 rows: ${JSON.stringify(writtenData)}`);
              } catch (writeError) {
                Logger.log(`❌ Error writing to ${sheetName}: ${writeError.message}`);
              }
            } else {
              // Sheet is empty (only header) - just write to row 2
              Logger.log(`📊 Sheet is empty - writing to row 2`);
              
              try {
                sheet.getRange(2, 1, report.data.length, report.data[0].length).setValues(report.data);
                Logger.log(`✅ Successfully wrote ${report.data.length} rows to ${sheetName}`);
                
                // Auto-resize columns after data is written
                // Auto-resize disabled
                Logger.log(`📏 Columns auto-resized`);
              } catch (writeError) {
                Logger.log(`❌ Error writing to ${sheetName}: ${writeError.message}`);
              }
            }
          } else {
            // Week exists and is older than latest - this shouldn't happen with current data
            Logger.log(`⚠️ Week ${newWeek} is OLDER than latest week (${latestWeek})`);
            Logger.log(`⚠️ This data seems to be historical - inserting at bottom to maintain history`);
            
            // Insert at bottom for historical data
            const lastRow = sheet.getLastRow();
            const insertRow = lastRow + 1;
            
            try {
              sheet.getRange(insertRow, 1, report.data.length, report.data[0].length).setValues(report.data);
              Logger.log(`✅ Appended historical week ${newWeek} to ${sheetName}`);
              // Auto-resize disabled
            } catch (writeError) {
              Logger.log(`❌ Error writing historical week: ${writeError.message}`);
            }
          }
        }
      } else {
        Logger.log(`No data to write to ${sheetName} - skipping empty sheet`);
        // For empty sheets, just ensure headers are set
        if (sheet.getLastRow() === 1) {
          Logger.log(`Sheet ${sheetName} is empty with only headers - this is normal`);
        }
      }
    });
    
    // Verify all sheets were created and have data
    Logger.log("=== Verifying Weekly Ranking Sheets ===");
    
    // Get all sheets that start with a country name and "Weekly Rankings"
    const allSheets = spreadsheet.getSheets();
    const weeklySheetNames = [];
    
    allSheets.forEach(sheet => {
      const sheetName = sheet.getName();
      // Include country-specific sheets (e.g., "米国週間ランキング")  
      // and standard weekly sheets
      if (sheetName.includes("週間ランキング") || 
          sheetName.includes("週間トップパフォーマー") ||
          sheetName.includes("国別週間パフォーマンス") ||
          sheetName.includes("デバイス週間パフォーマンス")) {
        weeklySheetNames.push(sheetName);
      }
    });
    
    Logger.log(`Found ${weeklySheetNames.length} weekly ranking sheets`);
    
    weeklySheetNames.forEach(sheetName => {
      const sheet = spreadsheet.getSheetByName(sheetName);
      if (sheet) {
        const lastRow = sheet.getLastRow();
        const lastCol = sheet.getLastColumn();
        Logger.log(`${sheetName}: ${lastRow} rows, ${lastCol} columns`);
        
        if (lastRow > 1) {
          const sampleData = sheet.getRange(2, 1, Math.min(2, lastRow - 1), lastCol).getValues();
          Logger.log(`${sheetName} sample data: ${JSON.stringify(sampleData)}`);
        } else {
          Logger.log(`${sheetName}: Empty sheet (only headers) - this is normal if no data available`);
        }
      } else {
        Logger.log(`${sheetName}: Sheet not found`);
      }
    });
    
    // Summary of sheet status
    Logger.log("=== Weekly Ranking Sheets Summary ===");
    let sheetsWithData = 0;
    let emptySheets = 0;
    
    weeklySheetNames.forEach(sheetName => {
      const sheet = spreadsheet.getSheetByName(sheetName);
      if (sheet) {
        const lastRow = sheet.getLastRow();
        if (lastRow > 1) {
          sheetsWithData++;
          Logger.log(`✅ ${sheetName}: Has data (${lastRow - 1} rows)`);
        } else {
          emptySheets++;
          Logger.log(`📋 ${sheetName}: Empty (headers only)`);
        }
      }
    });
    
    Logger.log(`Summary: ${sheetsWithData} sheets with data, ${emptySheets} empty sheets`);
    
    // Apply conditional formatting to weekly ranking sheets
    applyWeeklyRankingFormatting(spreadsheet);
    
    Logger.log("Weekly ranking data export completed");
    
  } catch (error) {
    Logger.log(`Weekly ranking export error: ${error.message}`);
    throw error;
  }
}

/**
 * Update "URLと平均掲載順位" sheet with the latest week's average rankings
 * @param {Object} weeklyResults - Weekly processing result object
 */
function updateUrlAverageRankingSheet(weeklyResults, dailyData) {
  try {
    Logger.log("Updating URL average ranking sheet...");
    
    if (!weeklyResults || !weeklyResults.weeklyData || weeklyResults.weeklyData.length === 0) {
      Logger.log("No weekly data available to update URL averages.");
      return;
    }
    
    const weeklyData = weeklyResults.weeklyData.filter(row => row && row.week && row.pageUrl);
    if (weeklyData.length === 0) {
      Logger.log("Weekly data does not include week keys or URLs. Skipping update.");
      return;
    }
    
    // Determine the most recent week key
    const weekKeys = Array.from(new Set(weeklyData.map(row => row.week)));
    if (weekKeys.length === 0) {
      Logger.log("No week keys found in weekly data. Skipping URL average update.");
      return;
    }
    
    const sortedWeeks = weekKeys
      .map(weekKey => ({ weekKey, date: getWeekDateFromKey(weekKey) }))
      .sort((a, b) => {
        if (a.date && b.date) {
          return b.date.getTime() - a.date.getTime();
        }
        if (a.date && !b.date) return -1;
        if (!a.date && b.date) return 1;
        return 0;
      });
    
    const latestEntry = sortedWeeks.find(entry => entry.date) || sortedWeeks[0];
    if (!latestEntry) {
      Logger.log("Could not determine latest week entry. Skipping URL average update.");
      return;
    }
    
    const latestWeekKey = latestEntry.weekKey;
    
    Logger.log(`Latest week for URL averages: ${latestWeekKey}`);
    
    const spreadsheet = getOrCreateSpreadsheet();
    let sheet = spreadsheet.getSheetByName("URLと平均掲載順位");
    
    if (!sheet) {
      Logger.log("URLと平均掲載順位 sheet not found. Creating new sheet...");
      sheet = spreadsheet.insertSheet("URLと平均掲載順位");
    }
    
    ensureUrlAverageSheetStructure(sheet);
    
    // Build average position map per URL for the latest week
    const urlAverageMap = calculateWeeklyAveragePositionByUrl(weeklyData, latestWeekKey);
    const totalUrlsWithData = Object.keys(urlAverageMap).length;
    Logger.log(`Calculated averages from weekly data for ${totalUrlsWithData} URLs.`);
    
    // Identify URL rows from column B (starting row 3)
    const lastRow = sheet.getLastRow();
    if (lastRow < 3) {
      Logger.log("No URL rows found (rows 3+). Skipping update.");
      return;
    }
    
    const urlRange = sheet.getRange(3, 2, lastRow - 2, 1);
    const urlValues = urlRange.getValues();
    const richTextValues = urlRange.getRichTextValues();
    const formulaValues = urlRange.getFormulas();
    const urlRows = [];
    urlValues.forEach((row, index) => {
      const cellValue = row[0];
      const richText = richTextValues[index] ? richTextValues[index][0] : null;
      const formula = formulaValues[index] ? formulaValues[index][0] : null;
      const extractedUrl = extractUrlFromCell(cellValue, richText, formula);
      const normalized = normalizeUrl(extractedUrl);
      if (normalized) {
        urlRows.push({
          rowIndex: index + 3,
          url: extractedUrl || cellValue,
          normalizedUrl: normalized
        });
      }
    });
    
    if (urlRows.length === 0) {
      Logger.log("No valid URLs listed in column B (rows 3+). Skipping update.");
      return;
    }
    
    // Find if the latest week already exists in the sheet (row 2)
    const lastColumn = sheet.getLastColumn();
    let targetColumnIndex = null;
    
    if (lastColumn >= 3) {
      const weekHeaderRange = sheet.getRange(2, 3, 1, lastColumn - 2);
      const weekHeaderValues = weekHeaderRange.getValues()[0] || [];
      weekHeaderValues.forEach((value, idx) => {
        if (value && value.toString() === latestWeekKey) {
          targetColumnIndex = idx + 3;
        }
      });
    }
    
    if (!targetColumnIndex) {
      // Insert new column at column C to keep the newest week first
      Logger.log(`Inserting new week column at C for ${latestWeekKey}`);
      sheet.insertColumnBefore(3);
      targetColumnIndex = 3;
    }
    
    // Update headers for the target column
    sheet.getRange(1, targetColumnIndex).setValue("平均掲載順位");
    sheet.getRange(1, targetColumnIndex).setBackground("#d9ead3");
    sheet.getRange(2, targetColumnIndex).setValue(latestWeekKey);
    sheet.getRange(2, targetColumnIndex).setBackground("#d9ead3");
    
    // Write averages for each URL row
    const numberFormatRows = Math.max(urlRows.length, 1);
    sheet.getRange(3, targetColumnIndex, numberFormatRows, 1).setNumberFormat("0");
    
    // Fill missing URLs by recalculating directly from daily data if available
    const missingUrlKeys = new Set();
    urlRows.forEach(rowInfo => {
      if (!(rowInfo.normalizedUrl in urlAverageMap)) {
        missingUrlKeys.add(rowInfo.normalizedUrl);
      }
    });
    
    if (missingUrlKeys.size > 0 && dailyData && dailyData.length > 0) {
      Logger.log(`Recalculating averages from daily data for ${missingUrlKeys.size} URLs not found in weekly aggregates.`);
      const targetWeekRange = getWeekRangeFromKey(latestWeekKey);
      if (targetWeekRange) {
        const fallbackMap = calculateWeeklyAveragePositionFromDailyData(
          dailyData,
          targetWeekRange,
          missingUrlKeys
        );
        Object.keys(fallbackMap).forEach(urlKey => {
          urlAverageMap[urlKey] = fallbackMap[urlKey];
        });
      } else {
        Logger.log("Could not parse week range from week key; skipping daily data fallback.");
      }
    }
    
    // Prepare data array for batch writing
    // If no position value exists, leave cell empty (blank) instead of 0
    const columnValues = urlRows.map(rowInfo => {
      const value = urlAverageMap[rowInfo.normalizedUrl];
      // If value exists and is a valid number, return rounded integer
      // If value doesn't exist (undefined/null), return empty string to leave cell blank
      if (typeof value === "number" && !isNaN(value)) {
        return Math.round(value);
      } else {
        return ''; // Empty string = blank cell (will trigger white color formatting)
      }
    });
    
    // Batch write data
    const dataRange = sheet.getRange(3, targetColumnIndex, columnValues.length, 1);
    const valuesArray = columnValues.map(v => [v === '' ? '' : v]);
    dataRange.setValues(valuesArray);
    dataRange.setNumberFormat("0");
    
    // Apply conditional formatting (same as last 3 weeks function)
    Logger.log("Applying conditional formatting to weekly average position column...");
    const positionRange = sheet.getRange(3, targetColumnIndex, urlRows.length, 1);
    const columnLetter = getColumnLetter(targetColumnIndex);
    
    // Get existing rules to preserve them
    const existingRules = sheet.getConditionalFormatRules();
    const newRules = [];
    
    // Copy existing rules
    existingRules.forEach(rule => {
      newRules.push(rule);
    });
    
    // Add rules for this column
    // White for position 0 (no data) - using whenNumberLessThan to catch 0
    // Note: Empty cells will remain unformatted (default background)
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberLessThan(0.5)
      .setBackground('#ffffff') // White
      .setRanges([positionRange])
      .build());
    
    // Green for positions 1-3
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(1, 3)
      .setBackground('#d9ead3') // Light green
      .setRanges([positionRange])
      .build());
    
    // Yellow for positions 4-15
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(4, 15)
      .setBackground('#fff2cc') // Light yellow
      .setRanges([positionRange])
      .build());
    
    // Red for positions > 15
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThan(15)
      .setBackground('#f4cccc') // Light red
      .setRanges([positionRange])
      .build());
    
    // Apply all rules
    try {
      sheet.setConditionalFormatRules(newRules);
      Logger.log(`Applied conditional formatting to column ${targetColumnIndex} for week ${latestWeekKey}`);
    } catch (formatError) {
      Logger.log(`Warning: Could not apply conditional formatting rules: ${formatError.message}`);
    }
    
    Logger.log(`Updated ${urlRows.length} URL rows for week ${latestWeekKey} (with color formatting).`);
    
    // Create/update chart for top 20 average position trend
    createTop10AverageChart(sheet);
    
  } catch (error) {
    Logger.log(`Error updating URL average ranking sheet: ${error.message}`);
  }
}

/**
 * Create/update country-specific URL average position sheets
 * Creates sheets for: 米国, カナダ, イギリス, オーストラリア
 * @param {Object} weeklyResults - Weekly processing result object
 * @param {Array} dailyData - Daily data (for fallback)
 */
function createCountryUrlAverageSheets(weeklyResults, dailyData) {
  try {
    Logger.log("=== Creating Country-Specific URL Average Position Sheets ===");
    
    if (!weeklyResults || !weeklyResults.weeklyData || weeklyResults.weeklyData.length === 0) {
      Logger.log("No weekly data available. Skipping country sheets creation.");
      return;
    }
    
    // Get URLs and structure from "URLと平均掲載順位" sheet
    const spreadsheet = getOrCreateSpreadsheet();
    const mainSheet = spreadsheet.getSheetByName("URLと平均掲載順位");
    
    if (!mainSheet) {
      Logger.log("URLと平均掲載順位 sheet not found. Cannot create country sheets.");
      return;
    }
    
    // Get URLs from main sheet
    const urlRows = getUrlsFromMainSheet(mainSheet);
    if (urlRows.length === 0) {
      Logger.log("No URLs found in main sheet. Skipping country sheets creation.");
      return;
    }
    
    // Get all week keys from main sheet
    const rawWeekKeys = getWeekKeysFromMainSheet(mainSheet);
    
    // Deduplicate week keys using normalized comparison and filter out numeric values
    const uniqueWeekKeys = [];
    const seenNormalized = new Set();
    
    rawWeekKeys.forEach(weekKey => {
      // Skip numeric values (not valid week strings)
      if (isNumericValue(weekKey)) {
        Logger.log(`Skipping numeric week key "${weekKey}" from main sheet`);
        return;
      }
      
      const normalized = normalizeWeekKeyForComparison(weekKey);
      if (normalized && !seenNormalized.has(normalized)) {
        seenNormalized.add(normalized);
        uniqueWeekKeys.push(weekKey); // Keep the first occurrence's original format
      }
    });
    
    Logger.log(`Found ${rawWeekKeys.length} week keys in main sheet, deduplicated to ${uniqueWeekKeys.length} unique weeks: ${uniqueWeekKeys.join(', ')}`);
    
    // Countries to create sheets for
    const countries = ["米国", "カナダ", "イギリス", "オーストラリア"];
    
    // Create/update each country sheet
    countries.forEach(country => {
      try {
        Logger.log(`\nProcessing ${country}...`);
        updateCountryUrlAverageSheet(country, weeklyResults.weeklyData, dailyData, urlRows, uniqueWeekKeys);
      } catch (countryError) {
        Logger.log(`Error processing ${country}: ${countryError.message}`);
      }
    });
    
    Logger.log("=== Country-Specific URL Average Position Sheets Complete ===");
    
  } catch (error) {
    Logger.log(`Error creating country URL average sheets: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
  }
}

/**
 * Get URLs from the main "URLと平均掲載順位" sheet
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @return {Array} Array of URL row objects
 */
function getUrlsFromMainSheet(sheet) {
  const urlRows = [];
  const lastRow = sheet.getLastRow();
  
  if (lastRow < 3) {
    return urlRows;
  }
  
  const urlRange = sheet.getRange(3, 2, lastRow - 2, 1);
  const urlValues = urlRange.getValues();
  const richTextValues = urlRange.getRichTextValues();
  const formulaValues = urlRange.getFormulas();
  
  urlValues.forEach((row, index) => {
    const cellValue = row[0];
    const richText = richTextValues[index] ? richTextValues[index][0] : null;
    const formula = formulaValues[index] ? formulaValues[index][0] : null;
    const extractedUrl = extractUrlFromCell(cellValue, richText, formula);
    const normalized = normalizeUrl(extractedUrl);
    if (normalized) {
      urlRows.push({
        rowIndex: index + 3,
        url: extractedUrl || cellValue,
        normalizedUrl: normalized
      });
    }
  });
  
  return urlRows;
}

/**
 * Get all week keys from the main "URLと平均掲載順位" sheet (row 2, columns C+)
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @return {Array} Array of week key strings
 */
function getWeekKeysFromMainSheet(sheet) {
  const weekKeys = [];
  const lastColumn = sheet.getLastColumn();
  
  if (lastColumn < 3) {
    return weekKeys;
  }
  
  const weekHeaderRange = sheet.getRange(2, 3, 1, lastColumn - 2);
  const weekHeaderValues = weekHeaderRange.getValues()[0] || [];
  
  weekHeaderValues.forEach((value, idx) => {
    if (value && value.toString().trim()) {
      const weekKey = value.toString().trim();
      // Skip numeric values that are not valid week strings
      if (isNumericValue(weekKey)) {
        Logger.log(`Skipping numeric week key from header: "${weekKey}"`);
        return;
      }
      // Skip header text
      if (weekKey !== "週" && weekKey !== "平均掲載順位") {
        weekKeys.push(weekKey);
      }
    }
  });
  
  return weekKeys;
}

/**
 * Update country-specific URL average position sheet
 * @param {string} country - Country name (米国, カナダ, イギリス, オーストラリア)
 * @param {Array} weeklyData - Weekly aggregated data
 * @param {Array} dailyData - Daily data (for fallback)
 * @param {Array} urlRows - URL rows from main sheet
 * @param {Array} weekKeys - Week keys to process
 */
function updateCountryUrlAverageSheet(country, weeklyData, dailyData, urlRows, weekKeys) {
  try {
    Logger.log(`Updating ${country} URL average position sheet...`);
    
    const spreadsheet = getOrCreateSpreadsheet();
    const sheetName = `${country}URL平均掲載順位`;
    let sheet = spreadsheet.getSheetByName(sheetName);
    
    if (!sheet) {
      Logger.log(`Creating new sheet: ${sheetName}`);
      sheet = spreadsheet.insertSheet(sheetName);
    }
    
    // Ensure sheet structure
    ensureCountryUrlAverageSheetStructure(sheet, urlRows);
    
    // Process each week
    const startColumn = 3; // Start at column C
    const existingWeekColumns = {};
    
    // Find existing week columns - check ALL week keys in the country sheet
    const lastColumn = sheet.getLastColumn();
    if (lastColumn >= 3) {
      const weekHeaderRange = sheet.getRange(2, 3, 1, lastColumn - 2);
      const weekHeaderValues = weekHeaderRange.getValues()[0] || [];
      weekHeaderValues.forEach((value, idx) => {
        if (value && value.toString().trim()) {
          const existingWeekKey = value.toString().trim();
          // Skip "週" header text
          if (existingWeekKey !== "週" && existingWeekKey !== "平均掲載順位") {
            // Store all existing week keys, not just those in weekKeys array
            existingWeekColumns[existingWeekKey] = idx + 3;
          }
        }
      });
    }
    
    Logger.log(`Found ${Object.keys(existingWeekColumns).length} existing week columns in ${country} sheet: ${Object.keys(existingWeekColumns).join(', ')}`);
    Logger.log(`Processing ${weekKeys.length} weeks from main sheet: ${weekKeys.join(', ')}`);
    
    // Cache for calculated averages to avoid recalculating the same week
    const calculatedAveragesCache = new Map();
    
    // Process each week
    weekKeys.forEach((weekKey, weekIndex) => {
      // Skip numeric values early (before expensive calculations)
      if (isNumericValue(weekKey)) {
        Logger.log(`Skipping numeric week key "${weekKey}" for ${country}`);
        return;
      }
      
      const targetColumn = startColumn + weekIndex;
      
      // Check if this week key already exists in the country sheet
      // Use exact match first, then try normalized comparison
      let existingColumn = existingWeekColumns[weekKey];
      
      // If exact match not found, try to find by normalizing both values
      if (!existingColumn) {
        const normalizedTarget = normalizeWeekKeyForComparison(weekKey);
        for (const [existingKey, colIndex] of Object.entries(existingWeekColumns)) {
          const normalizedExisting = normalizeWeekKeyForComparison(existingKey);
          // Try exact match after normalization
          if (normalizedTarget === normalizedExisting) {
            existingColumn = colIndex;
            Logger.log(`Found existing column for ${country} week ${weekKey} (matches existing "${existingKey}") at column ${existingColumn}`);
            break;
          }
        }
      } else {
        Logger.log(`Found exact match for ${country} week ${weekKey} at column ${existingColumn}`);
      }
      
      // Additional safety check: verify the target column doesn't already have this week
      if (!existingColumn && targetColumn <= lastColumn) {
        const targetWeekValue = sheet.getRange(2, targetColumn).getValue();
        if (targetWeekValue && targetWeekValue.toString().trim()) {
          const targetWeekKey = targetWeekValue.toString().trim();
          const normalizedTarget = normalizeWeekKeyForComparison(weekKey);
          const normalizedExisting = normalizeWeekKeyForComparison(targetWeekKey);
          if (normalizedTarget === normalizedExisting) {
            existingColumn = targetColumn;
            Logger.log(`Found existing week at target column ${targetColumn} for ${country} week ${weekKey}`);
          }
        }
      }
      
      // Check cache first to avoid recalculating the same week
      const cacheKey = `${country}|${weekKey}`;
      let countryAverageMap = calculatedAveragesCache.get(cacheKey);
      
      if (!countryAverageMap) {
        // Calculate country-specific averages for this week
        countryAverageMap = calculateCountryWeeklyAveragePositionByUrl(
          weeklyData, 
          weekKey, 
          country,
          dailyData
        );
        // Cache the result
        calculatedAveragesCache.set(cacheKey, countryAverageMap);
      } else {
        Logger.log(`Using cached averages for ${country} week ${weekKey}`);
      }
      
      // Prepare data array
      const columnValues = urlRows.map(rowInfo => {
        const value = countryAverageMap[rowInfo.normalizedUrl];
        if (typeof value === "number" && !isNaN(value)) {
          return Math.round(value);
        } else {
          return ''; // Empty string = blank cell
        }
      });
      
      if (existingColumn) {
        // Update existing column
        Logger.log(`Updating existing column for ${country} week ${weekKey} at column ${existingColumn}`);
        
        const dataRange = sheet.getRange(3, existingColumn, columnValues.length, 1);
        const valuesArray = columnValues.map(v => [v === '' ? '' : v]);
        dataRange.setValues(valuesArray);
        dataRange.setNumberFormat("0");
        
        // Apply conditional formatting
        applyCountryUrlAverageFormatting(sheet, existingColumn, urlRows.length);
      } else {
        // Final safety check: scan ALL columns one more time to see if this week already exists anywhere
        // (This catches cases where normalization might have missed it earlier)
        const normalizedTarget = normalizeWeekKeyForComparison(weekKey);
        const currentLastColumn = sheet.getLastColumn();
        let foundDuplicate = false;
        
        for (let scanCol = 3; scanCol <= currentLastColumn; scanCol++) {
          const scanValue = sheet.getRange(2, scanCol).getValue();
          if (scanValue && scanValue.toString().trim()) {
            const scanKey = scanValue.toString().trim();
            if (scanKey !== "週" && scanKey !== "平均掲載順位") {
              const normalizedScan = normalizeWeekKeyForComparison(scanKey);
              if (normalizedTarget === normalizedScan) {
                Logger.log(`WARNING: Week ${weekKey} already exists at column ${scanCol} (as "${scanKey}"). Updating existing column instead of creating duplicate.`);
                existingColumn = scanCol;
                foundDuplicate = true;
                // Update existing column instead
                const dataRange = sheet.getRange(3, existingColumn, columnValues.length, 1);
                const valuesArray = columnValues.map(v => [v === '' ? '' : v]);
                dataRange.setValues(valuesArray);
                dataRange.setNumberFormat("0");
                applyCountryUrlAverageFormatting(sheet, existingColumn, urlRows.length);
                break;
              }
            }
          }
        }
        
        // If we found a duplicate, skip creating new column
        if (foundDuplicate) {
          return; // Skip creating new column
        }
        
        // Always insert new week column at column C (position 3)
        // This will push all existing week columns to the right
        const newColumnPosition = 3; // Column C
        Logger.log(`Inserting new column for ${country} week ${weekKey} at column ${newColumnPosition} (column C)`);
        
        // Insert column before column C (which pushes existing columns to the right)
        sheet.insertColumnBefore(newColumnPosition);
        
        const actualTargetColumn = newColumnPosition;
        
        // Set headers
        sheet.getRange(1, actualTargetColumn, 1, 1).setValues([["平均掲載順位"]]);
        sheet.getRange(1, actualTargetColumn, 1, 1).setBackground("#d9ead3");
        sheet.getRange(2, actualTargetColumn, 1, 1).setValues([[weekKey]]);
        sheet.getRange(2, actualTargetColumn, 1, 1).setBackground("#d9ead3");
        
        // Write data
        const dataRange = sheet.getRange(3, actualTargetColumn, columnValues.length, 1);
        const valuesArray = columnValues.map(v => [v === '' ? '' : v]);
        dataRange.setValues(valuesArray);
        dataRange.setNumberFormat("0");
        
        // Apply conditional formatting
        applyCountryUrlAverageFormatting(sheet, actualTargetColumn, urlRows.length);
        
        Logger.log(`Created new column for ${country} week ${weekKey} at column ${actualTargetColumn}`);
      }
    });
    
    Logger.log(`Updated ${urlRows.length} URL rows for ${country} across ${weekKeys.length} weeks`);
    
    // Create/update chart for top 20 average position trend
    createTop10AverageChart(sheet);
    
  } catch (error) {
    Logger.log(`Error updating ${country} URL average sheet: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
  }
}

/**
 * Calculate country-specific weekly average position per URL
 * @param {Array} weeklyData - Weekly aggregated data
 * @param {string} targetWeekKey - Week key to filter
 * @param {string} country - Country name (米国, カナダ, イギリス, オーストラリア)
 * @param {Array} dailyData - Daily data (for fallback)
 * @return {Object} url -> average position
 */
function calculateCountryWeeklyAveragePositionByUrl(weeklyData, targetWeekKey, country, dailyData) {
  // Filter weekly data by country and week
  const countryWeekData = weeklyData.filter(row => 
    row && 
    row.week === targetWeekKey && 
    row.pageUrl && 
    row.country === country
  );
  
  // If we have data, calculate averages
  if (countryWeekData.length > 0) {
    return calculateWeeklyAveragePositionByUrl(countryWeekData, targetWeekKey);
  }
  
  // Fallback to daily data if no weekly data
  if (dailyData && dailyData.length > 0) {
    Logger.log(`No weekly data for ${country} week ${targetWeekKey}, calculating from daily data...`);
    const weekRange = getWeekRangeFromKey(targetWeekKey);
    if (weekRange) {
      // Filter daily data by country and week range
      const countryDailyData = dailyData.filter(row => 
        row && 
        row.pageUrl && 
        row.country === country &&
        row.date
      );
      
      if (countryDailyData.length > 0) {
        return calculateWeeklyAveragePositionFromDailyData(
          countryDailyData,
          weekRange,
          null // Calculate for all URLs
        );
      }
    }
  }
  
  return {}; // No data available
}

/**
 * Ensure country URL average sheet has baseline structure
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @param {Array} urlRows - URL rows to populate
 */
function ensureCountryUrlAverageSheetStructure(sheet, urlRows) {
  // Ensure minimum columns
  while (sheet.getLastColumn() < 3) {
    sheet.insertColumnAfter(sheet.getLastColumn());
  }
  
  // Set headers
  const headers = ["Date", "URL", "平均掲載順位"];
  headers.forEach((header, idx) => {
    const col = idx + 1;
    const current = sheet.getRange(1, col).getValue();
    if (!current) {
      sheet.getRange(1, col).setValue(header);
    }
  });
  
  sheet.getRange(1, 1, 1, sheet.getLastColumn()).setBackground("#d9ead3");
  
  // Set week header
  const weekCell = sheet.getRange(2, 3);
  if (!weekCell.getValue()) {
    weekCell.setValue("週");
  }
  
  const weekHeaderWidth = Math.max(1, sheet.getLastColumn() - 2);
  sheet.getRange(2, 3, 1, weekHeaderWidth).setBackground("#d9ead3");
  
  // Get Date column data from main "URLと平均掲載順位" sheet
  const spreadsheet = getOrCreateSpreadsheet();
  const mainSheet = spreadsheet.getSheetByName("URLと平均掲載順位");
  
  if (mainSheet) {
    const mainLastRow = mainSheet.getLastRow();
    if (mainLastRow >= 3) {
      // Get Date column data from main sheet (column A, rows 3+)
      const dateRange = mainSheet.getRange(3, 1, mainLastRow - 2, 1);
      const dateValues = dateRange.getValues();
      
      // Copy dates to country sheet column A
      const lastRow = sheet.getLastRow();
      const targetRowCount = Math.max(urlRows.length, dateValues.length);
      
      if (targetRowCount > 0) {
        // Prepare date values array (match the number of URL rows)
        const dateArray = [];
        for (let i = 0; i < urlRows.length; i++) {
          if (i < dateValues.length) {
            dateArray.push([dateValues[i][0]]);
          } else {
            dateArray.push(['']); // Empty if no date available
          }
        }
        
        // Write dates to column A starting from row 3
        if (dateArray.length > 0) {
          const countryDateRange = sheet.getRange(3, 1, dateArray.length, 1);
          countryDateRange.setValues(dateArray);
          Logger.log(`Copied ${dateArray.length} date values from main sheet to column A`);
        }
      }
    }
  }
  
  // Populate URLs in column B (if not already populated)
  const lastRow = sheet.getLastRow();
  if (lastRow < 3 || !sheet.getRange(3, 2).getValue()) {
    // Write URLs starting from row 3
    urlRows.forEach((rowInfo, index) => {
      const row = index + 3;
      sheet.getRange(row, 2).setValue(rowInfo.url);
    });
  }
}

/**
 * Apply conditional formatting to country URL average position column
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 * @param {number} columnIndex - Column index to format
 * @param {number} numRows - Number of data rows
 */
function applyCountryUrlAverageFormatting(sheet, columnIndex, numRows) {
  try {
    const positionRange = sheet.getRange(3, columnIndex, numRows, 1);
    const columnLetter = getColumnLetter(columnIndex);
    
    // Get existing rules
    const existingRules = sheet.getConditionalFormatRules();
    const newRules = [];
    
    // Copy existing rules
    existingRules.forEach(rule => {
      newRules.push(rule);
    });
    
    // Add rules for this column
    // White for position 0 (no data) - using whenNumberLessThan to catch 0
    // Note: Empty cells will remain unformatted (default background)
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberLessThan(0.5)
      .setBackground('#ffffff')
      .setRanges([positionRange])
      .build());
    
    // Green for positions 1-3
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(1, 3)
      .setBackground('#d9ead3')
      .setRanges([positionRange])
      .build());
    
    // Yellow for positions 4-15
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberBetween(4, 15)
      .setBackground('#fff2cc')
      .setRanges([positionRange])
      .build());
    
    // Red for positions > 15
    newRules.push(SpreadsheetApp.newConditionalFormatRule()
      .whenNumberGreaterThan(15)
      .setBackground('#f4cccc')
      .setRanges([positionRange])
      .build());
    
    // Apply rules
    try {
      sheet.setConditionalFormatRules(newRules);
    } catch (formatError) {
      Logger.log(`Warning: Could not apply conditional formatting: ${formatError.message}`);
    }
  } catch (error) {
    Logger.log(`Error applying formatting: ${error.message}`);
  }
}

/**
 * Ensure the "URLと平均掲載順位" sheet has baseline structure
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet
 */
function ensureUrlAverageSheetStructure(sheet) {
  while (sheet.getLastColumn() < 3) {
    sheet.insertColumnAfter(sheet.getLastColumn());
  }
  
  const headers = ["Date", "URL", "平均掲載順位"];
  headers.forEach((header, idx) => {
    const col = idx + 1;
    const current = sheet.getRange(1, col).getValue();
    if (!current) {
      sheet.getRange(1, col).setValue(header);
    }
  });
  
  sheet.getRange(1, 1, 1, sheet.getLastColumn()).setBackground("#d9ead3");
  
  const weekCell = sheet.getRange(2, 3);
  if (!weekCell.getValue()) {
    weekCell.setValue("週");
  }
  
  const weekHeaderWidth = Math.max(1, sheet.getLastColumn() - 2);
  sheet.getRange(2, 3, 1, weekHeaderWidth).setBackground("#d9ead3");
}

/**
 * Calculate weighted average position per URL for the specified week
 * @param {Array} weeklyData
 * @param {string} targetWeekKey
 * @return {Object} url -> average position
 */
function calculateWeeklyAveragePositionByUrl(weeklyData, targetWeekKey) {
  const stats = {};
  
  weeklyData.forEach(row => {
    if (!row || row.week !== targetWeekKey || !row.pageUrl) {
      return;
    }
    
    const urlKey = normalizeUrl(row.pageUrl);
    if (!urlKey) return;
    
    if (!stats[urlKey]) {
      stats[urlKey] = {
        weightedPosition: 0,
        weight: 0,
        fallbackSum: 0,
        count: 0
      };
    }
    
    const weight = row.impressions && row.impressions > 0
      ? row.impressions
      : (row.clicks && row.clicks > 0 ? row.clicks : (row.count || 1));
    
    stats[urlKey].weightedPosition += (row.position || 0) * weight;
    stats[urlKey].weight += weight;
    stats[urlKey].fallbackSum += (row.position || 0);
    stats[urlKey].count += 1;
  });
  
  const averages = {};
  Object.keys(stats).forEach(urlKey => {
    const stat = stats[urlKey];
    if (stat.weight > 0) {
      averages[urlKey] = Math.round(stat.weightedPosition / stat.weight);
    } else if (stat.count > 0) {
      averages[urlKey] = Math.round(stat.fallbackSum / stat.count);
    } else {
      averages[urlKey] = null;
    }
  });
  
  return averages;
}

/**
 * Calculate weighted average positions for specific URLs directly from daily data
 * @param {Array} dailyData
 * @param {Object} weekRange - { start: Date, end: Date }
 * @param {Set<string>} targetUrlKeys - normalized URLs to include
 * @return {Object} urlKey -> average
 */
function calculateWeeklyAveragePositionFromDailyData(dailyData, weekRange, targetUrlKeys) {
  if (!dailyData || !weekRange || !weekRange.start || !weekRange.end) {
    return {};
  }
  
  const stats = {};
  const hasTargetFilter = targetUrlKeys && targetUrlKeys.size > 0;
  
  dailyData.forEach(row => {
    if (!row || !row.pageUrl || !row.date) return;
    const normalizedUrl = normalizeUrl(row.pageUrl);
    if (!normalizedUrl) return;
    if (hasTargetFilter && !targetUrlKeys.has(normalizedUrl)) return;
    
    const rowDate = new Date(row.date);
    if (isNaN(rowDate.getTime())) return;
    if (rowDate < weekRange.start || rowDate > weekRange.end) return;
    
    if (!stats[normalizedUrl]) {
      stats[normalizedUrl] = {
        weightedPosition: 0,
        weight: 0,
        fallbackSum: 0,
        count: 0
      };
    }
    
    const weight = row.impressions && row.impressions > 0
      ? row.impressions
      : (row.clicks && row.clicks > 0 ? row.clicks : 1);
    
    stats[normalizedUrl].weightedPosition += (row.averagePosition || row.position || 0) * weight;
    stats[normalizedUrl].weight += weight;
    stats[normalizedUrl].fallbackSum += (row.averagePosition || row.position || 0);
    stats[normalizedUrl].count += 1;
  });
  
  const averages = {};
  Object.keys(stats).forEach(urlKey => {
    const stat = stats[urlKey];
    if (stat.weight > 0) {
      averages[urlKey] = Math.round(stat.weightedPosition / stat.weight);
    } else if (stat.count > 0) {
      averages[urlKey] = Math.round(stat.fallbackSum / stat.count);
    } else {
      averages[urlKey] = null;
    }
  });
  
  return averages;
}

/**
 * Convert a week key string to explicit start/end dates
 * @param {string} weekKey
 * @return {Object|null} { start: Date, end: Date }
 */
function getWeekRangeFromKey(weekKey) {
  if (!weekKey || typeof weekKey !== "string") {
    return null;
  }
  
  try {
    if (weekKey.includes("-")) {
      const parts = weekKey.split("-");
      const startParts = parts[0].trim().split("/");
      const endParts = parts[1].trim().split("/");
      
      if (startParts.length === 3 && endParts.length === 3) {
        const startDate = new Date(
          parseInt(startParts[0], 10),
          parseInt(startParts[1], 10) - 1,
          parseInt(startParts[2], 10)
        );
        const endDate = new Date(
          parseInt(endParts[0], 10),
          parseInt(endParts[1], 10) - 1,
          parseInt(endParts[2], 10)
        );
        endDate.setHours(23, 59, 59, 999);
        
        if (!isNaN(startDate.getTime()) && !isNaN(endDate.getTime())) {
          return { start: startDate, end: endDate };
        }
      }
    }
    
    const parsed = new Date(weekKey);
    if (!isNaN(parsed.getTime())) {
      const endDate = new Date(parsed);
      endDate.setDate(parsed.getDate() + 6);
      endDate.setHours(23, 59, 59, 999);
      return { start: parsed, end: endDate };
    }
  } catch (error) {
    Logger.log(`Failed to parse week range from key "${weekKey}": ${error.message}`);
  }
  
  return null;
}

/**
 * Convert column number to column letter (1 = A, 2 = B, etc.)
 * @param {number} columnNumber - Column number (1-based)
 * @return {string} Column letter(s)
 */
function getColumnLetter(columnNumber) {
  let result = '';
  while (columnNumber > 0) {
    columnNumber--;
    result = String.fromCharCode(65 + (columnNumber % 26)) + result;
    columnNumber = Math.floor(columnNumber / 26);
  }
  return result;
}

/**
 * Check if a value is numeric (not a valid week string)
 * @param {*} value - Value to check
 * @return {boolean} True if value is numeric, false if it's a valid week string
 */
function isNumericValue(value) {
  if (value === null || value === undefined) {
    return false;
  }
  
  // If it's already a number type, it's numeric
  if (typeof value === 'number') {
    return true;
  }
  
  // Convert to string and check
  const str = String(value).trim();
  
  // Empty string is not numeric
  if (str === '') {
    return false;
  }
  
  // Check if it's a pure number (integer or decimal)
  // This regex matches: optional sign, digits, optional decimal point and digits
  const numericPattern = /^-?\d+(\.\d+)?$/;
  if (numericPattern.test(str)) {
    return true;
  }
  
  // Check if it can be parsed as a number and is not NaN
  const parsed = parseFloat(str);
  if (!isNaN(parsed) && isFinite(parsed)) {
    // Additional check: if the string representation matches the parsed number exactly,
    // it's likely a number, not a week string
    if (String(parsed) === str || String(parseInt(str, 10)) === str) {
      return true;
    }
  }
  
  // If it contains date separators like "/", "-", or "_", it's likely a week string
  if (str.includes('/') || str.includes('-') || str.includes('_')) {
    return false;
  }
  
  // If it contains spaces (like "2025/11/10 - 2025/11/16"), it's likely a week string
  if (str.includes(' ')) {
    return false;
  }
  
  return false;
}

/**
 * Normalize week key for comparison (handles different date formats)
 * @param {string} weekKey - Week key string (e.g., "2024-11-03_2024-11-09" or "2024/11/03 - 2024/11/09")
 * @return {string} Normalized week key for comparison
 */
function normalizeWeekKeyForComparison(weekKey) {
  if (!weekKey || typeof weekKey !== "string") {
    return "";
  }
  
  // Remove all whitespace
  let normalized = weekKey.toString().trim().replace(/\s+/g, "");
  
  // Normalize date separators: convert "/" and "-" to consistent format
  // Handle format: "2024-11-03_2024-11-09" or "2024/11/03-2024/11/09" or "2024/11/03 - 2024/11/09"
  normalized = normalized.replace(/\//g, "-");
  
  // Handle underscore separator
  normalized = normalized.replace(/_/g, "-");
  
  // Remove any duplicate separators
  normalized = normalized.replace(/-+/g, "-");
  
  return normalized.toLowerCase();
}

/**
 * Normalize URLs for consistent matching
 * @param {string} url
 * @return {string}
 */
function normalizeUrl(url) {
  if (!url || typeof url !== "string") {
    return "";
  }
  
  let cleaned = url.trim();
  if (!cleaned) {
    return "";
  }
  
  try {
    if (!/^https?:\/\//i.test(cleaned)) {
      cleaned = `https://${cleaned}`;
    }
    
    const parsed = new URL(cleaned);
    let host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    let pathname = parsed.pathname || "/";
    
    pathname = pathname.replace(/\/+/g, "/");
    if (pathname.length > 1 && pathname.endsWith("/")) {
      pathname = pathname.slice(0, -1);
    }
    
    // Remove common AMP suffixes
    pathname = pathname.replace(/\/amp$/i, "");
    
    return `${host}${pathname}`.toLowerCase();
  } catch (error) {
    cleaned = cleaned
      .replace(/^https?:\/\//i, "")
      .replace(/^www\./i, "");
    
    cleaned = cleaned.split(/[?#]/)[0];
    cleaned = cleaned.replace(/\/+/g, "/");
    if (cleaned.length > 1 && cleaned.endsWith("/")) {
      cleaned = cleaned.slice(0, -1);
    }
    
    return cleaned.toLowerCase();
  }
}

/**
 * Extract actual URL from a cell that might contain rich text or HYPERLINK formulas
 * @param {any} cellValue
 * @param {GoogleAppsScript.Spreadsheet.RichTextValue} richTextValue
 * @param {string} formula
 * @return {string}
 */
function extractUrlFromCell(cellValue, richTextValue, formula) {
  try {
    if (richTextValue) {
      const directLink = richTextValue.getLinkUrl();
      if (directLink) {
        return directLink;
      }
      const runs = richTextValue.getRuns ? richTextValue.getRuns() : [];
      for (let i = 0; i < runs.length; i++) {
        const run = runs[i];
        if (run && typeof run.getLinkUrl === "function") {
          const runLink = run.getLinkUrl();
          if (runLink) {
            return runLink;
          }
        }
      }
    }
    
    if (formula && typeof formula === "string" && formula.toUpperCase().startsWith("=HYPERLINK")) {
      const match = formula.match(/=HYPERLINK\(\s*"([^"]+)"/i);
      if (match && match[1]) {
        return match[1];
      }
    }
    
    if (cellValue && typeof cellValue === "string") {
      return cellValue;
    }
    
    return "";
  } catch (error) {
    Logger.log(`Error extracting URL from cell: ${error.message}`);
    return typeof cellValue === "string" ? cellValue : "";
  }
}

/**
 * Convert a week key string (e.g., "2025/10/20 - 2025/10/26") to a Date for sorting
 * @param {string} weekKey
 * @return {Date|null}
 */
function getWeekDateFromKey(weekKey) {
  if (!weekKey || typeof weekKey !== "string") {
    return null;
  }
  
  try {
    if (weekKey.includes("-")) {
      const start = weekKey.split("-")[0].trim();
      const parts = start.split("/");
      if (parts.length === 3) {
        const year = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10) - 1;
        const day = parseInt(parts[2], 10);
        const date = new Date(year, month, day);
        return isNaN(date.getTime()) ? null : date;
      }
    }
    
    const parsed = new Date(weekKey);
    return isNaN(parsed.getTime()) ? null : parsed;
  } catch (error) {
    Logger.log(`Failed to parse week key "${weekKey}": ${error.message}`);
    return null;
  }
}

/**
 * Standalone function to calculate and update last 3 weeks average position
 * Can be called directly from Apps Script editor
 */
function runLast3WeeksAveragePosition() {
  try {
    Logger.log("=== Running Last 3 Weeks Average Position Calculation ===");
    
    // Get daily data from spreadsheet
    const dailyData = getDailyDataFromSpreadsheet();
    if (!dailyData || dailyData.length === 0) {
      Logger.log("No daily data found. Please run main() function first.");
      return;
    }
    
    // Process weekly data
    const weeklyProcessor = new WeeklyProcessor();
    const weeklyResults = weeklyProcessor.processWeeklyRankings(dailyData);
    
    if (!weeklyResults || !weeklyResults.weeklyData) {
      Logger.log("No weekly data available. Skipping.");
      return;
    }
    
    // Update URL average ranking sheet with last 3 weeks
    updateUrlAverageRankingSheetLast3Weeks(weeklyResults.weeklyData, dailyData);
    
    Logger.log("=== Last 3 Weeks Average Position Update Complete ===");
    
  } catch (error) {
    Logger.log(`Error in runLast3WeeksAveragePosition: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
  }
}

/**
 * Get weekly average position for the last 3 weeks (rounded to integers)
 * Calculates averages for: 11/3-11/9, 10/27-11/2, 10/20-10/26
 * @param {Array} weeklyData - Weekly aggregated data
 * @param {Array} dailyData - Daily data (for fallback calculation)
 * @return {Object} Object with week keys and URL average positions (integers)
 */
function getLast3WeeksAveragePosition(weeklyData, dailyData) {
  try {
    Logger.log("=== Calculating Last 3 Weeks Average Position ===");
    
    // Define the 3 target weeks (assuming 2025)
    const targetWeeks = [      
      "2025/11/10 - 2025/11/16",
      "2025/11/03 - 2025/11/09",  // Week 1: 11/3-11/9
      "2025/10/27 - 2025/11/02",  // Week 2: 10/27-11/2
    ];
    
    const result = {};
    
    targetWeeks.forEach(weekKey => {
      Logger.log(`Processing week: ${weekKey}`);
      
      // Try to calculate from weekly data first
      let urlAverageMap = calculateWeeklyAveragePositionByUrl(weeklyData, weekKey);
      
      // If no data found in weekly aggregates, try daily data fallback
      if (Object.keys(urlAverageMap).length === 0 && dailyData && dailyData.length > 0) {
        Logger.log(`No weekly data found for ${weekKey}, calculating from daily data...`);
        const weekRange = getWeekRangeFromKey(weekKey);
        if (weekRange) {
          urlAverageMap = calculateWeeklyAveragePositionFromDailyData(
            dailyData,
            weekRange,
            null // null means calculate for all URLs
          );
        }
      }
      
      // Round all values to integers
      const integerAverages = {};
      Object.keys(urlAverageMap).forEach(urlKey => {
        const value = urlAverageMap[urlKey];
        if (value !== null && value !== undefined && !isNaN(value)) {
          integerAverages[urlKey] = Math.round(value);
        } else {
          integerAverages[urlKey] = 0;
        }
      });
      
      result[weekKey] = integerAverages;
      Logger.log(`Calculated ${Object.keys(integerAverages).length} URL averages for ${weekKey}`);
    });
    
    Logger.log("=== Last 3 Weeks Average Position Calculation Complete ===");
    return result;
    
  } catch (error) {
    Logger.log(`Error calculating last 3 weeks average position: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
    return {};
  }
}

/**
 * Update URL average ranking sheet with last 3 weeks data (integer values)
 * @param {Array} weeklyData - Weekly aggregated data
 * @param {Array} dailyData - Daily data (for fallback)
 */
function updateUrlAverageRankingSheetLast3Weeks(weeklyData, dailyData) {
  try {
    Logger.log("=== Updating URL Average Ranking Sheet with Last 3 Weeks ===");
    
    // Get the last 3 weeks average positions
    const last3WeeksData = getLast3WeeksAveragePosition(weeklyData, dailyData);
    
    if (!last3WeeksData || Object.keys(last3WeeksData).length === 0) {
      Logger.log("No data available for last 3 weeks. Skipping update.");
      return;
    }
    
    const spreadsheet = getOrCreateSpreadsheet();
    let sheet = spreadsheet.getSheetByName("URLと平均掲載順位");
    
    if (!sheet) {
      Logger.log("URLと平均掲載順位 sheet not found. Creating new sheet...");
      sheet = spreadsheet.insertSheet("URLと平均掲載順位");
    }
    
    ensureUrlAverageSheetStructure(sheet);
    
    // Get URLs from column B (starting row 3)
    const lastRow = sheet.getLastRow();
    if (lastRow < 3) {
      Logger.log("No URL rows found (rows 3+). Skipping update.");
      return;
    }
    
    const urlRange = sheet.getRange(3, 2, lastRow - 2, 1);
    const urlValues = urlRange.getValues();
    const richTextValues = urlRange.getRichTextValues();
    const formulaValues = urlRange.getFormulas();
    const urlRows = [];
    
    urlValues.forEach((row, index) => {
      const cellValue = row[0];
      const richText = richTextValues[index] ? richTextValues[index][0] : null;
      const formula = formulaValues[index] ? formulaValues[index][0] : null;
      const extractedUrl = extractUrlFromCell(cellValue, richText, formula);
      const normalized = normalizeUrl(extractedUrl);
      if (normalized) {
        urlRows.push({
          rowIndex: index + 3,
          url: extractedUrl || cellValue,
          normalizedUrl: normalized
        });
      }
    });
    
    if (urlRows.length === 0) {
      Logger.log("No valid URLs listed in column B (rows 3+). Skipping update.");
      return;
    }
    
    // Define week order (newest first)
    const weekKeys = [
      "2025/11/10 - 2025/11/16",
      "2025/11/03 - 2025/11/09",  // Week 1: 11/3-11/9 (newest)
      "2025/10/27 - 2025/11/02",  // Week 2: 10/27-11/2
    ];
    
    // Check existing columns and update/create as needed
    const lastColumn = sheet.getLastColumn();
    const startColumn = 3; // Start at column C
    
    // First, find all existing week columns
    const existingWeekColumns = {};
    if (lastColumn >= 3) {
      const weekHeaderRange = sheet.getRange(2, 3, 1, lastColumn - 2);
      const weekHeaderValues = weekHeaderRange.getValues()[0] || [];
      weekHeaderValues.forEach((value, idx) => {
        if (value && value.toString()) {
          const weekKeyStr = value.toString();
          if (weekKeys.includes(weekKeyStr)) {
            existingWeekColumns[weekKeyStr] = idx + 3;
          }
        }
      });
    }
    
    // Prepare all data for batch writing
    const columnsToCreate = [];
    const columnsToUpdate = [];
    const allDataValues = [];
    const allFormatRanges = [];
    
    // Process each week in order
    weekKeys.forEach((weekKey, weekIndex) => {
      const targetColumn = startColumn + weekIndex;
      const weekData = last3WeeksData[weekKey] || {};
      
      // Prepare data array for this column (batch write)
      // If no position value exists, leave cell empty (blank) instead of 0
      const columnValues = urlRows.map(rowInfo => {
        const value = weekData[rowInfo.normalizedUrl];
        // If value exists and is a valid number, return rounded integer
        // If value doesn't exist (undefined/null), return empty string to leave cell blank
        if (typeof value === "number" && !isNaN(value)) {
          return Math.round(value);
        } else {
          return ''; // Empty string = blank cell (will trigger white color formatting)
        }
      });
      allDataValues.push(columnValues);
      
      if (existingWeekColumns[weekKey]) {
        // Update existing column
        const existingColumn = existingWeekColumns[weekKey];
        Logger.log(`Updating existing column for week ${weekKey} at column ${existingColumn}`);
        columnsToUpdate.push({
          column: existingColumn,
          weekKey: weekKey,
          values: columnValues
        });
        allFormatRanges.push({
          range: sheet.getRange(3, existingColumn, urlRows.length, 1),
          column: existingColumn
        });
      } else {
        // Need to create new column
        columnsToCreate.push({
          column: targetColumn,
          weekKey: weekKey,
          values: columnValues
        });
        allFormatRanges.push({
          range: sheet.getRange(3, targetColumn, urlRows.length, 1),
          column: targetColumn
        });
      }
    });
    
    // Batch create columns first
    if (columnsToCreate.length > 0) {
      Logger.log(`Creating ${columnsToCreate.length} new columns...`);
      const maxColumn = Math.max(...columnsToCreate.map(c => c.column));
      const currentLastColumn = sheet.getLastColumn();
      if (maxColumn > currentLastColumn) {
        const columnsToInsert = maxColumn - currentLastColumn;
        for (let i = 0; i < columnsToInsert; i++) {
          sheet.insertColumnAfter(sheet.getLastColumn());
        }
      }
      
        // Batch write headers and data for new columns
      columnsToCreate.forEach(colInfo => {
        // Set headers
        sheet.getRange(1, colInfo.column, 1, 1).setValues([["平均掲載順位"]]);
        sheet.getRange(1, colInfo.column, 1, 1).setBackground("#d9ead3");
        sheet.getRange(2, colInfo.column, 1, 1).setValues([[colInfo.weekKey]]);
        sheet.getRange(2, colInfo.column, 1, 1).setBackground("#d9ead3");
        
        // Batch write data
        // Empty strings will leave cells blank (triggers white color formatting)
        const dataRange = sheet.getRange(3, colInfo.column, colInfo.values.length, 1);
        const valuesArray = colInfo.values.map(v => [v === '' ? '' : v]);
        dataRange.setValues(valuesArray);
        dataRange.setNumberFormat("0");
        
        Logger.log(`Created new column for week ${colInfo.weekKey} at column ${colInfo.column}`);
      });
    }
    
    // Batch update existing columns
    if (columnsToUpdate.length > 0) {
      Logger.log(`Updating ${columnsToUpdate.length} existing columns...`);
      columnsToUpdate.forEach(colInfo => {
        // Batch write data
        // Empty strings will leave cells blank (triggers white color formatting)
        const dataRange = sheet.getRange(3, colInfo.column, colInfo.values.length, 1);
        const valuesArray = colInfo.values.map(v => [v === '' ? '' : v]);
        dataRange.setValues(valuesArray);
        dataRange.setNumberFormat("0");
      });
    }
    
    // Apply conditional formatting to all 3 week columns (same as 全サイトデータ sheet)
    Logger.log("Applying conditional formatting to weekly average position columns...");
    
    // Get existing rules to preserve them
    const existingRules = sheet.getConditionalFormatRules();
    const newRules = [];
    
    // Copy existing rules (limit to avoid too many rules)
    existingRules.forEach(rule => {
      newRules.push(rule);
    });
    
    // Add rules for each week column
    weekKeys.forEach((weekKey, weekIndex) => {
      const targetColumn = startColumn + weekIndex;
      const existingColumn = existingWeekColumns[weekKey] || targetColumn;
      const positionRange = sheet.getRange(3, existingColumn, urlRows.length, 1);
      const columnLetter = getColumnLetter(existingColumn);
      
      // White for position 0 (no data) - use lessThan(0.5) to catch 0
      // Note: Empty cells will remain unformatted (default background)
      // This rule must be first to take priority
      newRules.push(SpreadsheetApp.newConditionalFormatRule()
        .whenNumberLessThan(0.5)
        .setBackground('#ffffff') // White
        .setRanges([positionRange])
        .build());
      
      // Green for positions 1-3
      newRules.push(SpreadsheetApp.newConditionalFormatRule()
        .whenNumberBetween(1, 3)
        .setBackground('#d9ead3') // Light green
        .setRanges([positionRange])
        .build());
      
      // Yellow for positions 4-15
      newRules.push(SpreadsheetApp.newConditionalFormatRule()
        .whenNumberBetween(4, 15)
        .setBackground('#fff2cc') // Light yellow
        .setRanges([positionRange])
        .build());
      
      // Red for positions > 15
      newRules.push(SpreadsheetApp.newConditionalFormatRule()
        .whenNumberGreaterThan(15)
        .setBackground('#f4cccc') // Light red
        .setRanges([positionRange])
        .build());
      
      Logger.log(`Added conditional formatting rules for column ${existingColumn} (week ${weekKey})`);
    });
    
    // Apply all rules at once (only if we have new rules)
    if (newRules.length > existingRules.length) {
      try {
        sheet.setConditionalFormatRules(newRules);
        Logger.log(`Applied conditional formatting to ${weekKeys.length} week columns`);
      } catch (formatError) {
        Logger.log(`Warning: Could not apply all conditional formatting rules: ${formatError.message}`);
        // Try to apply just the new rules if total is too many
        if (newRules.length > 100) {
          Logger.log("Too many conditional formatting rules. Skipping formatting to avoid timeout.");
        }
      }
    }
    
    Logger.log(`Updated ${urlRows.length} URL rows for last 3 weeks (integer values with color formatting)`);
    
    // Create/update chart for top 20 average position trend
    createTop10AverageChart(sheet);
    
    Logger.log("=== URL Average Ranking Sheet Update Complete ===");
    
  } catch (error) {
    Logger.log(`Error updating URL average ranking sheet for last 3 weeks: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
  }
}

/**
 * Compare data between main function and weekly processor
 */
function compareDataSources() {
  try {
    Logger.log("=== Comparing Data Sources ===");
    
    // Check what main function would see
    Logger.log("1. Checking main function data source...");
    const mainData = getDailyDataFromSpreadsheet();
    Logger.log(`Main function would see: ${mainData.length} records`);
    
    // Check what's actually in the sheet
    Logger.log("2. Checking actual sheet content...");
    const spreadsheet = getOrCreateSpreadsheet();
    const allSitesSheet = spreadsheet.getSheetByName("全サイトデータ");
    
    if (allSitesSheet) {
      const lastRow = allSitesSheet.getLastRow();
      const lastCol = allSitesSheet.getLastColumn();
      Logger.log(`Sheet reports: ${lastRow} rows, ${lastCol} columns`);
      
      // Check for empty rows
      let actualDataRows = 0;
      for (let row = 2; row <= lastRow; row++) {
        const rowData = allSitesSheet.getRange(row, 1, 1, lastCol).getValues()[0];
        const hasData = rowData.some(cell => cell !== null && cell !== undefined && cell !== "");
        if (hasData) {
          actualDataRows++;
        }
      }
      Logger.log(`Actual data rows (non-empty): ${actualDataRows}`);
    }
    
  } catch (error) {
    Logger.log(`Error comparing data sources: ${error.message}`);
  }
}

/**
 * Check current state of 全サイトデータ sheet
 */
function checkAllSitesDataSheet() {
  try {
    Logger.log("=== Checking 全サイトデータ Sheet ===");
    
    const spreadsheet = getOrCreateSpreadsheet();
    const allSitesSheet = spreadsheet.getSheetByName("全サイトデータ");
    
    if (!allSitesSheet) {
      Logger.log("全サイトデータ sheet not found");
      return;
    }
    
    const lastRow = allSitesSheet.getLastRow();
    const lastCol = allSitesSheet.getLastColumn();
    Logger.log(`全サイトデータ sheet: ${lastRow} rows, ${lastCol} columns`);
    
    if (lastRow > 1) {
      // Show first few rows
      Logger.log("First 5 rows of data:");
      for (let row = 1; row <= Math.min(5, lastRow); row++) {
        const rowData = allSitesSheet.getRange(row, 1, 1, lastCol).getValues()[0];
        Logger.log(`Row ${row}: ${rowData.slice(0, 5).join(" | ")}${lastCol > 5 ? "..." : ""}`);
      }
      
      // Show last few rows
      if (lastRow > 5) {
        Logger.log("Last 3 rows of data:");
        for (let row = Math.max(1, lastRow - 2); row <= lastRow; row++) {
          const rowData = allSitesSheet.getRange(row, 1, 1, lastCol).getValues()[0];
          Logger.log(`Row ${row}: ${rowData.slice(0, 5).join(" | ")}${lastCol > 5 ? "..." : ""}`);
        }
      }
    } else {
      Logger.log("No data in 全サイトデータ sheet");
    }
    
  } catch (error) {
    Logger.log(`Error checking 全サイトデータ sheet: ${error.message}`);
  }
}


/**
 * Parse Japanese date format to JavaScript Date object
 * @param {string} dateStr - Date string in format "2025年10月24日"
 * @return {Date} JavaScript Date object
 */
function parseJapaneseDate(dateStr) {
  try {
    // Handle Japanese date format: "2025年10月24日"
    if (typeof dateStr === 'string' && dateStr.includes('年') && dateStr.includes('月') && dateStr.includes('日')) {
      const match = dateStr.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
      if (match) {
        const year = parseInt(match[1]);
        const month = parseInt(match[2]) - 1; // JavaScript months are 0-based
        const day = parseInt(match[3]);
        return new Date(year, month, day);
      }
    }
    
    // Handle standard date formats
    if (dateStr instanceof Date) {
      return dateStr;
    }
    
    // Try to parse as standard date
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      Logger.log(`Warning: Could not parse date: ${dateStr}`);
      return new Date(); // Return current date as fallback
    }
    
    return date;
    
  } catch (error) {
    Logger.log(`Error parsing date "${dateStr}": ${error.message}`);
    return new Date(); // Return current date as fallback
  }
}

/**
 * Apply conditional formatting to weekly ranking sheets
 * @param {Object} spreadsheet - Google Spreadsheet object
 */
function applyWeeklyRankingFormatting(spreadsheet) {
  try {
    Logger.log("Applying conditional formatting to weekly ranking sheets...");
    
    // Get all sheets (including country-specific ones)
    const allSheets = spreadsheet.getSheets();
    const weeklySheetNames = [];
    
    allSheets.forEach(sheet => {
      const sheetName = sheet.getName();
      if (sheetName.includes("Weekly Rankings") || 
          sheetName.includes("Weekly Top Performers") ||
          sheetName.includes("Country Weekly Performance") ||
          sheetName.includes("Device Weekly Performance")) {
        weeklySheetNames.push(sheetName);
      }
    });
    
    Logger.log(`Found ${weeklySheetNames.length} weekly ranking sheets to format`);
    
    weeklySheetNames.forEach(sheetName => {
      const sheet = spreadsheet.getSheetByName(sheetName);
      if (!sheet) {
        Logger.log(`Sheet ${sheetName} not found, skipping formatting`);
        return;
      }
      
      const lastRow = sheet.getLastRow();
      if (lastRow <= 1) {
        Logger.log(`Sheet ${sheetName} has no data, skipping formatting`);
        return;
      }
      
      Logger.log(`Applying formatting to ${sheetName} (${lastRow} rows)`);
      
      const rules = [];
      
      // Apply formatting to all weekly ranking sheets (including country-specific)
      if (sheetName.includes("週間ランキング") || sheetName.includes("週間トップパフォーマー") || sheetName.includes("国別週間パフォーマンス") || sheetName.includes("デバイス週間パフォーマンス")) {
        // Position column (9) - Green for 1-3, Yellow for 4-15, Red for 15+
        const positionColumn = 9;
        const positionRange = sheet.getRange(2, positionColumn, lastRow - 1, 1);
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberLessThanOrEqualTo(3)
          .setBackground('#d9ead3') // Light green
          .setRanges([positionRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberBetween(4, 15)
          .setBackground('#fff2cc') // Light yellow
          .setRanges([positionRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberGreaterThan(15)
          .setBackground('#f4cccc') // Light red
          .setRanges([positionRange])
          .build());
        
        // Clicks column (6) - Green for 15+, Yellow for 4-15, Red for <4
        const clicksColumn = 6;
        const clicksRange = sheet.getRange(2, clicksColumn, lastRow - 1, 1);
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberGreaterThanOrEqualTo(15)
          .setBackground('#d9ead3') // Light green
          .setRanges([clicksRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberBetween(4, 14)
          .setBackground('#fff2cc') // Light yellow
          .setRanges([clicksRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberLessThan(4)
          .setBackground('#f4cccc') // Light red
          .setRanges([clicksRange])
          .build());
        
        // Impressions column (7) - Green for 15+, Yellow for 4-15, Red for <4
        const impressionsColumn = 7;
        const impressionsRange = sheet.getRange(2, impressionsColumn, lastRow - 1, 1);
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberGreaterThanOrEqualTo(15)
          .setBackground('#d9ead3') // Light green
          .setRanges([impressionsRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberBetween(4, 14)
          .setBackground('#fff2cc') // Light yellow
          .setRanges([impressionsRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberLessThan(4)
          .setBackground('#f4cccc') // Light red
          .setRanges([impressionsRange])
          .build());
        
        // CTR column (8) - Green for 10%+, Yellow for 4-10%, Red for <4%
        const ctrColumn = 8;
        const ctrRange = sheet.getRange(2, ctrColumn, lastRow - 1, 1);
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberGreaterThanOrEqualTo(0.10) // 10%
          .setBackground('#d9ead3') // Light green
          .setRanges([ctrRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberBetween(0.04, 0.099) // 4-10%
          .setBackground('#fff2cc') // Light yellow
          .setRanges([ctrRange])
          .build());
        
        rules.push(SpreadsheetApp.newConditionalFormatRule()
          .whenNumberLessThan(0.04) // <4%
          .setBackground('#f4cccc') // Light red
          .setRanges([ctrRange])
          .build());
      }
      
      // Apply all rules
      if (rules.length > 0) {
        sheet.setConditionalFormatRules(rules);
        Logger.log(`Applied ${rules.length} formatting rules to ${sheetName}`);
      }
    });
    
    Logger.log("Weekly ranking sheets formatting completed");
    
  } catch (error) {
    Logger.log(`Error applying weekly ranking formatting: ${error.message}`);
  }
}


/**
 * Create trend charts for weekly ranking data
 * @param {Array} historicalData - Array of weekly data with trends
 * @param {string} sheetName - Name of the sheet to add charts to
 */
function createTrendCharts(historicalData, sheetName) {
  try {
    Logger.log(`Creating trend charts for ${sheetName}...`);
    
    const spreadsheet = getOrCreateSpreadsheet();
    const sheet = spreadsheet.getSheetByName(sheetName);
    
    if (!sheet || !historicalData || historicalData.length === 0) {
      Logger.log(`Cannot create charts: sheet not found or no data`);
      return;
    }
    
    // Group data by week
    const weeklyData = groupWeeklyData(historicalData);
    let weeks = Object.keys(weeklyData).sort();
    
    if (weeks.length < 2) {
      Logger.log(`Need at least 2 weeks of data for charts. Current weeks: ${weeks.length}`);
      return;
    }
    
    // Filter weeks to show only the last 3 months (approximately 12 weeks)
    // NOTE: Charts can technically display unlimited weeks, but we limit to 12 for readability
    // If you need more weeks, increase maxWeeksToDisplay (e.g., 24 for 6 months, 52 for 1 year)
    const maxWeeksToDisplay = 12;
    if (weeks.length > maxWeeksToDisplay) {
      weeks = weeks.slice(-maxWeeksToDisplay); // Take the most recent 12 weeks
      Logger.log(`Filtered to show last ${maxWeeksToDisplay} weeks out of ${Object.keys(weeklyData).length} total weeks`);
    }
    
    // Prepare data for chart with headers
    const chartData = [
      ['週', 'クリック数', 'インプレッション数', 'CTR (%)', '平均ポジション'], // Header row
      ...weeks.map(week => {
        const weekRecords = weeklyData[week];
        const totals = weekRecords.reduce((acc, row) => ({
          clicks: acc.clicks + parseInt(row[5]) || 0,
          impressions: acc.impressions + parseInt(row[6]) || 0
        }), { clicks: 0, impressions: 0 });
        
        const avgCTR = weekRecords.length > 0 
          ? (totals.clicks / totals.impressions) * 100 : 0;
        const avgPosition = weekRecords.length > 0
          ? weekRecords.reduce((sum, row) => sum + parseFloat(row[8]) || 0, 0) / weekRecords.length
          : 0;
        
        return [week, totals.clicks, totals.impressions, avgCTR, avgPosition];
      })
    ];
    
    // Create combined chart with all metrics
    // Determine the title based on how many weeks are shown
    const totalWeeks = weeks.length;
    const title = totalWeeks >= 12 
      ? '週間パフォーマンストレンド（過去3ヶ月）' 
      : `週間パフォーマンストレンド（過去${totalWeeks}週）`;
    
    Logger.log(`Chart title will be: "${title}"`);
    
    // Get current last row to place chart safely
    const currentLastRow = sheet.getLastRow();
    const safeChartRow = Math.max(5, currentLastRow + 2); // At least row 5, or 2 rows after data
    
    // Find the last column with data in row 3 (first data row after headers)
    const headerRow = 1;
    const firstDataRow = 3;
    const lastColumnInRow3 = sheet.getLastColumn();
    
    // Check what columns actually have data in row 3
    let lastDataColumn = 1; // Start with column 1
    const row3Data = sheet.getRange(firstDataRow, 1, 1, lastColumnInRow3).getValues()[0];
    
    // Find the rightmost column with data in row 3
    for (let col = lastColumnInRow3; col >= 1; col--) {
      if (row3Data[col - 1] !== null && row3Data[col - 1] !== '') {
        lastDataColumn = col;
        break;
      }
    }
    
    Logger.log(`Row 3 last data column: ${lastDataColumn}`);
    
    // Position chart after the last data column (with 1 column gap)
    const chartStartColumn = lastDataColumn + 1;
    
    // Debug: Log chart data to verify impression values
    Logger.log(`Chart data sample (first 3 rows):`);
    chartData.slice(0, 3).forEach((row, idx) => {
      Logger.log(`Row ${idx}: Week=${row[0]}, Clicks=${row[1]}, Impressions=${row[2]}, CTR=${row[3]}, Position=${row[4]}`);
    });
    
    const combinedChart = createCombinedTrendChart(sheet, chartData, {
      title: title,
      //position: { col: 3, row: chartStartColumn + 1, width: 800, height: 450 }
      position: { col: 3, row: 26, width: 800, height: 450 }
    });
    
    Logger.log(`Created combined trend chart with all metrics`);
    
    Logger.log(`Trend charts created successfully for ${sheetName}`);
    
  } catch (error) {
    Logger.log(`Error creating trend charts: ${error.message}`);
  }
}

/**
 * Group historical data by week
 * @param {Array} data - Historical data
 * @return {Object} Data grouped by week
 */
function groupWeeklyData(data) {
  const grouped = {};
  
  data.forEach(row => {
    const week = row[0];
    if (!grouped[week]) {
      grouped[week] = [];
    }
    grouped[week].push(row);
  });
  
  return grouped;
}

/**
 * Create a line chart
 * @param {Object} sheet - Google Sheet
 * @param {Array} data - Chart data
 * @param {Object} options - Chart options
 * @return {Object} Chart object
 */
function createLineChart(sheet, data, options) {
  try {
    // Get the next available column to store chart data temporarily
    const lastCol = sheet.getLastColumn();
    const chartDataCol = lastCol + 2; // Start 2 columns after existing data
    
    // Ensure we have enough columns
    if (data[0].length > 0) {
      // Set chart data in temporary columns
      const chartDataRange = sheet.getRange(1, chartDataCol, data.length, data[0].length);
      chartDataRange.setValues(data);
      
      // Build ranges for each data series
      const dataRanges = options.yAxisColumns.map(colIndex => {
        return sheet.getRange(1, chartDataCol + colIndex, data.length, 1);
      });
      
      // Build chart
      const chartBuilder = sheet.newChart()
        .setChartType(Charts.ChartType.LINE)
        .addRange(sheet.getRange(1, chartDataCol, data.length, 1)); // X-axis (week)
      
      // Add data series
      dataRanges.forEach((range, index) => {
        chartBuilder.addRange(range);
      });
      
      chartBuilder
        .setPosition(options.position.col, 
                     options.position.row, 0, 0)
        .setOption('title', options.title)
        .setOption('width', options.position.width)
        .setOption('height', options.position.height)
        .setOption('legend', { position: 'top' })
        .setOption('hAxis', { title: '週' })
        .setOption('vAxis', { title: '値' });
      
      if (options.seriesNames) {
        chartBuilder.setOption('series', {
          0: { labelInLegend: options.seriesNames[0] },
          1: { labelInLegend: options.seriesNames[1] }
        });
      }
      
      const chart = chartBuilder.build();
      sheet.insertChart(chart);
      
      Logger.log(`Successfully created line chart at column ${chartDataCol}`);
      return chart;
    }
    
    return null;
    
  } catch (error) {
    Logger.log(`Error creating line chart: ${error.message}`);
    return null;
  }
}

/**
 * Create a column chart
 * @param {Object} sheet - Google Sheet
 * @param {Array} data - Chart data
 * @param {Object} options - Chart options
 * @return {Object} Chart object
 */
function createColumnChart(sheet, data, options) {
  try {
    // Get the next available column to store chart data temporarily
    const lastCol = sheet.getLastColumn();
    const chartDataCol = lastCol + 2; // Start 2 columns after existing data
    
    // Ensure we have enough columns
    if (data[0].length > 0) {
      // Set chart data in temporary columns
      const chartDataRange = sheet.getRange(1, chartDataCol, data.length, data[0].length);
      chartDataRange.setValues(data);
      
      // Build chart
      const chartBuilder = sheet.newChart()
        .setChartType(Charts.ChartType.COLUMN)
        .addRange(sheet.getRange(1, chartDataCol, data.length, 1)) // X-axis (week)
        .addRange(sheet.getRange(1, chartDataCol + options.yAxisColumn, data.length, 1)) // Y-axis (value)
        .setPosition(options.position.col, 
                     options.position.row, 0, 0)
        .setOption('title', options.title)
        .setOption('width', options.position.width)
        .setOption('height', options.position.height)
        .setOption('legend', { position: 'top' })
        .setOption('hAxis', { title: '週' })
        .setOption('vAxis', { title: '値' })
        .setOption('colors', ['#4285f4', '#ea4335']);
      
      const chart = chartBuilder.build();
      sheet.insertChart(chart);
      
      Logger.log(`Successfully created column chart at column ${chartDataCol}`);
      return chart;
    }
    
    return null;
    
  } catch (error) {
    Logger.log(`Error creating column chart: ${error.message}`);
    return null;
  }
}

/**
 * Create a combined trend chart with all metrics
 * @param {Object} sheet - Google Sheet
 * @param {Array} data - Chart data with headers
 * @param {Object} options - Chart options
 * @return {Object} Chart object
 */
function createCombinedTrendChart(sheet, data, options) {
  try {
    if (data.length === 0) {
      Logger.log("No data provided for chart");
      return null;
    }
    
    // Use fixed column positions for chart data (columns 20-24)
    const chartDataCol = 20;
    const numCols = data[0].length; // Should be 5: Week, Clicks, Impressions, CTR, Position
    const numRows = data.length; // Including header
    
    Logger.log(`Chart data dimensions: ${numRows} rows × ${numCols} columns, starting at column ${chartDataCol}`);
    
    // Ensure we have valid dimensions
    if (numRows === 0 || numCols === 0 || !data[0] || !Array.isArray(data[0])) {
      Logger.log("Invalid chart data structure");
      return null;
    }
    
    // Remove ALL existing charts before creating a new one
    // (Chart titles aren't accessible via getOptions() in Apps Script)
    const existingCharts = sheet.getCharts();
    Logger.log(`Found ${existingCharts.length} existing charts on sheet. Removing all before creating new chart.`);
    
    if (existingCharts.length > 0) {
      existingCharts.forEach((chart, index) => {
        sheet.removeChart(chart);
        Logger.log(`Removed existing chart ${index + 1}`);
      });
      Logger.log(`All existing charts removed. Creating new chart with title: "${options.title}"`);
    } else {
      Logger.log(`No existing charts found. Creating new chart with title: "${options.title}"`);
    }
    
    const wasUpdate = existingCharts.length > 0;
    
    // Set chart data in temporary columns (including headers)
    const chartDataRange = sheet.getRange(1, chartDataCol, numRows, numCols);
    chartDataRange.setValues(data);
    
    // Build ranges for each data series
    const xAxisRange = sheet.getRange(1, chartDataCol, numRows, 1); // Week column
    const clicksRange = sheet.getRange(1, chartDataCol + 1, numRows, 1); // Clicks
    const impressionsRange = sheet.getRange(1, chartDataCol + 2, numRows, 1); // Impressions
    const ctrRange = sheet.getRange(1, chartDataCol + 3, numRows, 1); // CTR
    const positionRange = sheet.getRange(1, chartDataCol + 4, numRows, 1); // Position
    
    // Debug: Log the impression values that will be plotted
    const impressionValues = data.map(row => row[2]); // Impressions is column 3 (index 2)
    Logger.log(`Impression values for chart: ${impressionValues.join(', ')}`);
    
    // Build chart with combo type (lines with different Y-axes)
    const chartBuilder = sheet.newChart()
      .setChartType(Charts.ChartType.LINE)
      .addRange(xAxisRange) // X-axis: Week
      .addRange(clicksRange) // Series 1: Clicks
      .addRange(impressionsRange) // Series 2: Impressions
      .addRange(ctrRange) // Series 3: CTR
      .addRange(positionRange) // Series 4: Position
      .setPosition(options.position.col, options.position.row, 0, 0)
      .setOption('title', options.title)
      .setOption('width', options.position.width)
      .setOption('height', options.position.height)
      .setOption('legend', { position: 'top' })
      .setOption('hAxis', { 
        title: '週',
        titleTextStyle: { bold: true },
        gridlines: { color: '#cccccc', count: 3 }
      })
      .setOption('vAxes', {
        0: { 
          title: 'クリック数 / インプレッション数',
          titleTextStyle: { bold: true },
          viewWindow: { min: -10, max: null },
          logScale: false,
          gridlines: { count: 5 }
        },
        1: {
          title: 'CTR (%) / 平均ポジション',
          titleTextStyle: { bold: true },
          viewWindow: { min: -1, max: null },
          format: 'decimal',
          scaleType: 'linear',
          gridlines: { count: 5 }
        }
      })
      .setOption('series', {
        0: { 
          labelInLegend: 'クリック数',
          type: 'line',
          targetAxisIndex: 0,
          lineWidth: 3,
          pointSize: 8,
          color: '#4285f4',
          visibleInLegend: true,
          pointShape: 'circle'
        },
        1: { 
          labelInLegend: 'インプレッション数',
          type: 'line',
          targetAxisIndex: 0,
          lineWidth: 2,
          color: '#ea4335',
          pointSize: 4
        },
        2: { 
          labelInLegend: 'CTR (%)',
          type: 'line',
          targetAxisIndex: 1,
          lineWidth: 3,
          pointSize: 8,
          color: '#34a853',
          lineDashStyle: [5, 5], // Dashed line
          visibleInLegend: true,
          pointShape: 'diamond'
        },
        3: { 
          labelInLegend: '平均ポジション',
          type: 'line',
          targetAxisIndex: 1,
          lineWidth: 2,
          color: '#fbbc04',
          pointSize: 4
        }
      })
      .setOption('tooltip', { trigger: 'selection' })
      .setOption('animation', { duration: 1000, easing: 'out' })
      .setOption('useFirstColumnAsDomain', true); // Use first column as X-axis
    
    const chart = chartBuilder.build();
    sheet.insertChart(chart);
    
    // Track whether this was an update or creation (wasUpdate is set earlier)
    const action = wasUpdate ? "Updated" : "Created";
    Logger.log(`Successfully ${action.toLowerCase()} combined trend chart`);
    return chart;
    
  } catch (error) {
    Logger.log(`Error creating combined trend chart: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
    return null;
  }
}

/**
 * Create trend dashboard with comprehensive charts
 */
function createTrendDashboard() {
  try {
    Logger.log("Creating Trend Dashboard...");
    
    const spreadsheet = getOrCreateSpreadsheet();
    
    // Check if dashboard already exists
    let dashboardSheet = spreadsheet.getSheetByName("Trend Dashboard");
    if (!dashboardSheet) {
      dashboardSheet = spreadsheet.insertSheet("Trend Dashboard");
      
      // Add header
      dashboardSheet.getRange(1, 1, 1, 6).setValues([[
        "Metric", "Week 1", "Week 2", "Week 3", "Week 4", "Trend"
      ]]);
      dashboardSheet.getRange(1, 1, 1, 6).setBackground('#d9ead3'); // Light green background
    }
    
    // Get data from weekly ranking sheets
    const countries = ["米国", "カナダ", "イギリス", "オーストラリア", "ニュージーランド", "シンガポール"];
    
    countries.forEach((country, index) => {
      const sheet = spreadsheet.getSheetByName(`${country}週間ランキング`);
      if (!sheet) return;
      
      const lastRow = sheet.getLastRow();
      if (lastRow <= 1) return;
      
      const data = sheet.getRange(2, 1, lastRow - 1, 16).getValues();
      
      // Add summary to dashboard
      const summaryRow = index + 2;
      dashboardSheet.getRange(summaryRow, 1, 1, 6).setValues([[
        country, 
        data.filter(r => r[0].includes("10/13")).length,
        data.filter(r => r[0].includes("10/20")).length,
        data.filter(r => r[0].includes("10/27")).length,
        data.filter(r => r[0].includes("11/03")).length,
        "→"
      ]]);
    });
    
    // Auto-resize columns
    // Auto-resize disabled
    
    Logger.log("Trend Dashboard created successfully");
    
  } catch (error) {
    Logger.log(`Error creating trend dashboard: ${error.message}`);
    throw error;
  }
}

/**
 * Calculate the average of top 20 lowest average positions for each week
 * Gets week data directly from the provided sheet (works for main sheet or country sheets)
 * Limits to last 12 weeks if available, only includes weeks with valid data
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The sheet to analyze (main or country sheet)
 * @return {Array} Array of [weekKey, average] pairs
 */
function calculateTop10AverageByWeek(sheet) {
  try {
    // Get week keys directly from this sheet (works for any sheet including country sheets)
    let weekKeys = getWeekKeysFromMainSheet(sheet);
    if (weekKeys.length === 0) {
      Logger.log("No week keys found for top 20 average calculation");
      return [];
    }
    
    // Deduplicate week keys using normalized comparison
    const uniqueWeekKeys = [];
    const seenNormalized = new Set();
    
    weekKeys.forEach(weekKey => {
      const normalized = normalizeWeekKeyForComparison(weekKey);
      if (normalized && !seenNormalized.has(normalized)) {
        seenNormalized.add(normalized);
        uniqueWeekKeys.push(weekKey); // Keep the first occurrence's original format
      }
    });
    
    Logger.log(`Deduplicated ${weekKeys.length} week keys to ${uniqueWeekKeys.length} unique weeks`);
    
    // Limit to last 12 weeks (if available)
    // First, sort by date (newest first) to get the most recent weeks
    const weekKeysWithDates = uniqueWeekKeys.map(weekKey => ({
      weekKey: weekKey,
      date: getWeekDateFromKey(weekKey)
    })).filter(item => item.date !== null)
      .sort((a, b) => b.date.getTime() - a.date.getTime()); // Newest first
    
    // Take only the last 12 weeks (most recent) - or all available if less than 12
    const recentWeeks = weekKeysWithDates.slice(0, 12);
    const finalWeekKeys = recentWeeks.map(item => item.weekKey);
    
    Logger.log(`Limiting chart to last ${finalWeekKeys.length} weeks (out of ${weekKeysWithDates.length} total unique weeks available in sheet)`);
    
    const lastRow = sheet.getLastRow();
    if (lastRow < 3) {
      Logger.log("No URL rows found for top 20 average calculation");
      return [];
    }
    
    const urlCount = lastRow - 2; // URLs start at row 3
    
    // Get week column indices
    const lastColumn = sheet.getLastColumn();
    const weekData = [];
    const processedWeeks = new Set(); // Track processed weeks to avoid duplicates
    
    finalWeekKeys.forEach(weekKey => {
      // Skip if we've already processed this week (normalized)
      const normalized = normalizeWeekKeyForComparison(weekKey);
      if (processedWeeks.has(normalized)) {
        Logger.log(`Skipping duplicate week ${weekKey} (already processed)`);
        return;
      }
      
      // Find column for this week using normalized comparison
      let weekColumn = null;
      if (lastColumn >= 3) {
        const weekHeaderRange = sheet.getRange(2, 3, 1, lastColumn - 2);
        const weekHeaderValues = weekHeaderRange.getValues()[0] || [];
        const normalizedTarget = normalizeWeekKeyForComparison(weekKey);
        
        weekHeaderValues.forEach((value, idx) => {
          if (value && value.toString().trim()) {
            const existingKey = value.toString().trim();
            // Skip header text
            if (existingKey !== "週" && existingKey !== "平均掲載順位") {
              const normalizedExisting = normalizeWeekKeyForComparison(existingKey);
              // Use normalized comparison to find matching column
              if (normalizedTarget === normalizedExisting && !weekColumn) {
                weekColumn = idx + 3;
              }
            }
          }
        });
      }
      
      if (!weekColumn) {
        Logger.log(`Week column not found for ${weekKey}`);
        return;
      }
      
      // Mark this week as processed
      processedWeeks.add(normalized);
      
      // Get all position values for this week
      const positionRange = sheet.getRange(3, weekColumn, urlCount, 1);
      const positionValues = positionRange.getValues().map(row => row[0]);
      
      // Filter valid numbers (exclude empty strings, null, 0)
      const validPositions = positionValues
        .filter(val => typeof val === 'number' && !isNaN(val) && val > 0)
        .sort((a, b) => a - b); // Sort ascending (lowest first)
      
      if (validPositions.length === 0) {
        Logger.log(`No valid positions found for week ${weekKey}`);
        return;
      }
      
      // Get top 20 lowest positions
      const top20 = validPositions.slice(0, Math.min(20, validPositions.length));
      
      // Calculate average
      const sum = top20.reduce((acc, val) => acc + val, 0);
      const average = sum / top20.length;
      
      weekData.push([weekKey, Math.round(average * 100) / 100]); // Round to 2 decimals
      
      Logger.log(`Week ${weekKey}: Top 20 average = ${average.toFixed(2)} (from ${top20.length} URLs)`);
    });
    
    // Sort by week date (oldest first for chart)
    weekData.sort((a, b) => {
      const dateA = getWeekDateFromKey(a[0]);
      const dateB = getWeekDateFromKey(b[0]);
      if (dateA && dateB) {
        return dateA.getTime() - dateB.getTime();
      }
      return 0;
    });
    
    // Final deduplication check on the weekData array itself
    const finalWeekData = [];
    const finalSeen = new Set();
    weekData.forEach(([weekKey, average]) => {
      // Skip if weekKey is a number (not a valid week string)
      if (isNumericValue(weekKey)) {
        Logger.log(`Skipping numeric value "${weekKey}" - not a valid week string`);
        return;
      }
      
      const normalized = normalizeWeekKeyForComparison(weekKey);
      if (!finalSeen.has(normalized)) {
        finalSeen.add(normalized);
        finalWeekData.push([weekKey, average]);
      }
    });
    
    Logger.log(`Final chart data: ${finalWeekData.length} unique weeks (removed ${weekData.length - finalWeekData.length} duplicates/non-weeks)`);
    
    return finalWeekData;
    
  } catch (error) {
    Logger.log(`Error calculating top 20 average by week: ${error.message}`);
    return [];
  }
}

/**
 * Create or update chart showing top 20 average position trend
 * Chart is positioned at I4 (column 9, row 4)
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - The sheet to add chart to
 */
function createTop10AverageChart(sheet) {
  try {
    Logger.log("Creating/updating top 20 average position chart...");
    
    // Calculate top 20 averages for each week
    const weekData = calculateTop10AverageByWeek(sheet);
    
    if (weekData.length === 0) {
      Logger.log("No data available for chart. Skipping chart creation.");
      return;
    }
    
    Logger.log(`Chart data: ${weekData.length} weeks`);
    
    // Remove existing chart (remove all charts and recreate to ensure clean state)
    // Since we can't reliably identify charts by title, we'll remove all and recreate
    const existingCharts = sheet.getCharts();
    if (existingCharts.length > 0) {
      // Try to identify and remove the top 20 average chart
      // Check charts near I4 position (column 9, row 4)
      existingCharts.forEach(chart => {
        try {
          const chartTitle = chart.getOptions().get('title');
          if (chartTitle && (chartTitle.toString().includes('トップ20平均') || 
                             chartTitle.toString().includes('トップ10平均') ||
                             chartTitle.toString().includes('Top 20') ||
                             chartTitle.toString().includes('Top 10'))) {
            sheet.removeChart(chart);
            Logger.log("Removed existing top 20 average chart");
          }
        } catch (e) {
          // If we can't check, continue - we'll recreate anyway
        }
      });
    }
    
    // Prepare chart data: [Week, Average Position]
    // Use a hidden column to store chart data (after last data column)
    const lastColumn = sheet.getLastColumn();
    const chartDataStartCol = lastColumn + 2; // Start 2 columns after data
    
    // Ensure we have enough columns
    const neededColumns = chartDataStartCol + 1 - sheet.getMaxColumns();
    if (neededColumns > 0) {
      // Add columns if needed (though this shouldn't be necessary usually)
      for (let i = 0; i < neededColumns; i++) {
        sheet.insertColumnAfter(sheet.getLastColumn());
      }
    }
    
    // Filter out any numeric week values before creating chart data
    const validWeekData = weekData.filter(([week, avg]) => {
      // Skip if week is a number (not a valid week string)
      if (isNumericValue(week)) {
        Logger.log(`Skipping numeric week value "${week}" in chart data`);
        return false;
      }
      return true;
    });
    
    if (validWeekData.length === 0) {
      Logger.log("No valid week strings found for chart. Skipping chart creation.");
      return;
    }
    
    // Write chart data: Header row + data rows
    // Ensure week values are written as text strings (prepend with apostrophe or format as text)
    const chartData = [
      ['週', 'トップ10平均順位'], // Header
      ...validWeekData.map(([week, avg]) => [String(week), avg]) // Convert week to string explicitly
    ];
    
    const chartDataRange = sheet.getRange(1, chartDataStartCol, chartData.length, 2);
    chartDataRange.setValues(chartData);
    
    // Format week column as text to ensure it displays as labels, not numbers
    const weekColumnRange = sheet.getRange(1, chartDataStartCol, chartData.length, 1); // Include header
    weekColumnRange.setNumberFormat('@'); // '@' means text format - forces text interpretation
    
    // Format average column as number
    const avgColumnRange = sheet.getRange(2, chartDataStartCol + 1, validWeekData.length, 1);
    avgColumnRange.setNumberFormat('0'); // Integer format
    
    // Hide the chart data columns
    sheet.hideColumns(chartDataStartCol, 2);
    
    // Create chart ranges
    const xAxisRange = sheet.getRange(2, chartDataStartCol, validWeekData.length, 1); // Weeks (skip header)
    const yAxisRange = sheet.getRange(2, chartDataStartCol + 1, validWeekData.length, 1); // Averages (skip header)
    
    // Build chart
    const chartBuilder = sheet.newChart()
      .setChartType(Charts.ChartType.LINE)
      .addRange(xAxisRange) // X-axis: Weeks
      .addRange(yAxisRange) // Y-axis: Top 10 Average Position
      .setPosition(9, 4, 0, 0) // I4 = column 9, row 4
      .setOption('title', 'トップ20平均掲載順位の推移')
      .setOption('width', 600)
      .setOption('height', 400)
      .setOption('legend', { position: 'none' }) // Single series, no legend needed
      .setOption('hAxis', { 
        title: '週',
        titleTextStyle: { bold: true },
        slantedText: false,
        slantedTextAngle: 0,
        format: 'text' // Ensure X-axis treats values as text/categories
      })
      .setOption('vAxis', { 
        title: '平均掲載順位',
        titleTextStyle: { bold: true },
        viewWindow: { min: 0 },
        direction: -1 // Invert Y-axis (lower is better, so show lower at top)
      })
      .setOption('lineWidth', 3)
      .setOption('pointSize', 5)
      .setOption('colors', ['#4285f4']); // Blue line
    
    const chart = chartBuilder.build();
    sheet.insertChart(chart);
    
    Logger.log(`Successfully created top 20 average position chart at I4`);
    
  } catch (error) {
    Logger.log(`Error creating top 20 average chart: ${error.message}`);
    Logger.log(`Error stack: ${error.stack}`);
  }
}