import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { getFile } from '@/lib/db';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { BarChart3 } from 'lucide-react';

interface DataVisualizationProps {
  uploadedFiles: Array<{ id: string; name: string }>;
}

export function DataVisualization({ uploadedFiles }: DataVisualizationProps) {
  const [selectedFile, setSelectedFile] = useState<string>('');
  const [chartType, setChartType] = useState<'line' | 'bar'>('bar');
  const [data, setData] = useState<any[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [xColumn, setXColumn] = useState<string>('');
  const [yColumn, setYColumn] = useState<string>('');

  useEffect(() => {
    if (selectedFile) {
      loadFileData(selectedFile);
    }
  }, [selectedFile]);

  const loadFileData = async (fileId: string) => {
    try {
      const file = await getFile(fileId);
      if (!file) return;

      // Parse CSV data (assuming CSV for now)
      const text = new TextDecoder().decode(file.data);
      const lines = text.split('\n').filter(l => l.trim());
      
      if (lines.length === 0) return;

      const headers = lines[0].split(',').map(h => h.trim());
      setColumns(headers);
      setXColumn(headers[0] || '');
      setYColumn(headers[1] || '');

      const parsedData = lines.slice(1).map(line => {
        const values = line.split(',');
        const row: any = {};
        headers.forEach((header, idx) => {
          const value = values[idx]?.trim();
          // Try to parse as number
          const numValue = parseFloat(value);
          row[header] = isNaN(numValue) ? value : numValue;
        });
        return row;
      });

      setData(parsedData);
    } catch (error) {
      console.error('Failed to parse file:', error);
    }
  };

  if (uploadedFiles.length === 0) {
    return (
      <Alert>
        <BarChart3 className="h-4 w-4" />
        <AlertDescription>
          Upload CSV or Excel files to visualize your FP&A data.
        </AlertDescription>
      </Alert>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Data Visualization</CardTitle>
        <CardDescription>Visualize your uploaded financial data</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="text-sm font-medium mb-2 block">Dataset</label>
            <Select value={selectedFile} onValueChange={setSelectedFile}>
              <SelectTrigger>
                <SelectValue placeholder="Select file" />
              </SelectTrigger>
              <SelectContent>
                {uploadedFiles.map((file) => (
                  <SelectItem key={file.id} value={file.id}>
                    {file.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div>
            <label className="text-sm font-medium mb-2 block">Chart Type</label>
            <Select value={chartType} onValueChange={(v) => setChartType(v as 'line' | 'bar')}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="bar">Bar Chart</SelectItem>
                <SelectItem value="line">Line Chart</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>

        {columns.length > 0 && (
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium mb-2 block">X-Axis</label>
              <Select value={xColumn} onValueChange={setXColumn}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {columns.map((col) => (
                    <SelectItem key={col} value={col}>
                      {col}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="text-sm font-medium mb-2 block">Y-Axis</label>
              <Select value={yColumn} onValueChange={setYColumn}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {columns.map((col) => (
                    <SelectItem key={col} value={col}>
                      {col}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {data.length > 0 && xColumn && yColumn && (
          <div className="h-[300px] mt-4">
            <ResponsiveContainer width="100%" height="100%">
              {chartType === 'line' ? (
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey={xColumn} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey={yColumn} stroke="hsl(var(--primary))" strokeWidth={2} />
                </LineChart>
              ) : (
                <BarChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey={xColumn} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey={yColumn} fill="hsl(var(--primary))" />
                </BarChart>
              )}
            </ResponsiveContainer>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
